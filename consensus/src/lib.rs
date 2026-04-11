// Copyright(C) Facebook, Inc. and its affiliates.
use config::{Committee, ConsensusProtocol, Stake};
use crypto::Hash as _;
use crypto::{coin_threshold, recover_coin, Digest, PublicKey};
use log::{debug, info, log_enabled, warn};
use primary::{Certificate, Round};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc::{unbounded_channel, Receiver, Sender, UnboundedSender};

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;

/// The representation of the DAG in memory.
type Dag = HashMap<Round, HashMap<PublicKey, (Digest, Certificate)>>;
type CoinCache = HashMap<Round, Round>;

/// Number of DAG rounds in one consensus wave.
/// Keep this even so the leader interval (`ROUNDS_PER_WAVE / 2`) is integral.
const ROUNDS_PER_WAVE: Round = 4;

struct CoinComputationInput {
    round: Round,
    authorities: Vec<PublicKey>,
    threshold: usize,
    shares: Vec<(PublicKey, Vec<u8>)>,
}

/// The state that needs to be persisted for crash-recovery.
struct State {
    /// The last committed round.
    last_committed_round: Round,
    // Keeps the last committed round for each authority. This map is used to clean up the dag and
    // ensure we don't commit twice the same certificate.
    last_committed: HashMap<PublicKey, Round>,
    /// Keeps the latest committed certificate (and its parents) for every authority. Anything older
    /// must be regularly cleaned up through the function `update`.
    dag: Dag,
}

impl State {
    fn new(genesis: Vec<Certificate>) -> Self {
        let genesis = genesis
            .into_iter()
            .map(|x| (x.origin(), (x.digest(), x)))
            .collect::<HashMap<_, _>>();

        Self {
            last_committed_round: 0,
            last_committed: genesis.iter().map(|(x, (_, y))| (*x, y.round())).collect(),
            dag: [(0, genesis)].iter().cloned().collect(),
        }
    }

    /// Update and clean up internal state base on committed certificates.
    fn update(&mut self, certificate: &Certificate, gc_depth: Round) {
        self.last_committed
            .entry(certificate.origin())
            .and_modify(|r| *r = max(*r, certificate.round()))
            .or_insert_with(|| certificate.round());

        let last_committed_round = *self.last_committed.values().max().unwrap();
        self.last_committed_round = last_committed_round;

        // TODO: This cleanup is dangerous: we need to ensure consensus can receive idempotent replies
        // from the primary. Here we risk cleaning up a certificate and receiving it again later.
        for (name, round) in &self.last_committed {
            self.dag.retain(|r, authorities| {
                authorities.retain(|n, _| n != name || r >= round);
                !authorities.is_empty() && r + gc_depth >= last_committed_round
            });
        }
    }
}

pub struct Consensus {
    /// The committee information.
    committee: Committee,
    /// The depth of the garbage collector.
    gc_depth: Round,
    /// The consensus leader election mode.
    consensus_protocol: ConsensusProtocol,

    /// Receives new certificates from the primary. The primary should send us new certificates only
    /// if it already sent us its whole history.
    rx_primary: Receiver<Certificate>,
    /// Outputs the sequence of ordered certificates to the primary (for cleanup and feedback).
    tx_primary: Sender<Certificate>,
    /// Outputs the sequence of ordered certificates to the application layer.
    tx_output: Sender<Certificate>,

    /// The genesis certificates.
    genesis: Vec<Certificate>,
}

impl Consensus {
    pub fn spawn(
        committee: Committee,
        gc_depth: Round,
        rx_primary: Receiver<Certificate>,
        tx_primary: Sender<Certificate>,
        tx_output: Sender<Certificate>,
    ) {
        Self::spawn_with_protocol(
            committee,
            gc_depth,
            ConsensusProtocol::RoundRobin,
            rx_primary,
            tx_primary,
            tx_output,
        );
    }

    pub fn spawn_with_protocol(
        committee: Committee,
        gc_depth: Round,
        consensus_protocol: ConsensusProtocol,
        rx_primary: Receiver<Certificate>,
        tx_primary: Sender<Certificate>,
        tx_output: Sender<Certificate>,
    ) {
        tokio::spawn(async move {
            Self {
                committee: committee.clone(),
                gc_depth,
                consensus_protocol,
                rx_primary,
                tx_primary,
                tx_output,
                genesis: Certificate::genesis(&committee),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        debug_assert!(ROUNDS_PER_WAVE >= 2 && ROUNDS_PER_WAVE % 2 == 0);

        // The consensus state (everything else is immutable).
        let mut state = State::new(self.genesis.clone());
        let mut coin_cache = CoinCache::new();
        let mut pending_coin_rounds = HashSet::new();
        let (coin_result_tx, mut coin_result_rx) = unbounded_channel::<(Round, Option<Round>)>();

        // Listen to incoming certificates.
        while let Some(certificate) = self.rx_primary.recv().await {
            while let Ok((round, coin)) = coin_result_rx.try_recv() {
                pending_coin_rounds.remove(&round);
                if let Some(coin) = coin {
                    coin_cache.insert(round, coin);
                }
            }

            debug!("Processing {:?}", certificate);
            let round = certificate.round();

            // Add the new certificate to the local storage.
            state
                .dag
                .entry(round)
                .or_insert_with(HashMap::new)
                .insert(certificate.origin(), (certificate.digest(), certificate));
            self.schedule_common_coin(
                round,
                &state.dag,
                &mut pending_coin_rounds,
                &coin_cache,
                &coin_result_tx,
            );
            // Try to order the dag to commit using the section-6 rule from DAG构建(1).md:
            // - trigger only when round r ends and r is a multiple of 4;
            // - elect leader at round r-3;
            // - require same-author chain b3 (r-3), b2 (r-2), b1 (r-1);
            // - require embedded QC links b2->b3 and b1->b2.
            let commit_round = round;

            if commit_round < ROUNDS_PER_WAVE || commit_round % ROUNDS_PER_WAVE != 0 {
                continue;
            }

            // We only consider a round ended for commit purposes once we have a quorum for that round.
            if !self.round_has_quorum(commit_round, &state.dag) {
                continue;
            }

            // Get the certificate of the wave leader. If we already ordered this leader,
            // there is nothing to do.
            let leader_round = commit_round - (ROUNDS_PER_WAVE - 1);
            if leader_round <= state.last_committed_round {
                continue;
            }

            if matches!(self.consensus_protocol, ConsensusProtocol::CommonCoin)
                && !coin_cache.contains_key(&commit_round)
            {
                if let Some(input) = self.common_coin_input(commit_round, &state.dag) {
                    if let Some(coin) = recover_coin(
                        &input.authorities,
                        input.threshold,
                        input.round,
                        &input.shares,
                    ) {
                        coin_cache.insert(commit_round, coin);
                    }
                }
            }

            let (_, leader) = match self.leader(leader_round, commit_round, &state.dag, &coin_cache) {
                Some(x) => x,
                None => continue,
            };

            let b3 = leader.clone();
            let Some(b2) = self.certificate_by_author(leader_round + 1, b3.origin(), &state.dag) else {
                continue;
            };
            let Some(b1) = self.certificate_by_author(leader_round + 2, b3.origin(), &state.dag) else {
                continue;
            };

            if !self.embedded_qc_links(b2, &b3, commit_round)
                || !self.embedded_qc_links(b1, b2, commit_round)
            {
                debug!("Leader {:?} does not satisfy b3->b2->b1 QC chain", b3);
                continue;
            }

            debug!("Leader {:?} satisfies section-6 commit rule", b3);
            let mut sequence = Vec::new();
            for x in self.order_dag(&b3, &state) {
                // Update and clean up internal state.
                state.update(&x, self.gc_depth);

                // Add the certificate to the sequence.
                sequence.push(x);
            }

            // Log the latest committed round of every authority (for debug).
            if log_enabled!(log::Level::Debug) {
                for (name, round) in &state.last_committed {
                    debug!("Latest commit of {}: Round {}", name, round);
                }
            }

            // Output the sequence in the right order.
            for certificate in sequence {
                #[cfg(not(feature = "benchmark"))]
                info!("Committed {}", certificate.header);

                #[cfg(feature = "benchmark")]
                for digest in certificate.header.payload.keys() {
                    // NOTE: This log entry is used to compute performance.
                    info!("Committed {} -> {:?}", certificate.header, digest);
                }

                self.tx_primary
                    .send(certificate.clone())
                    .await
                    .expect("Failed to send certificate to primary");

                if let Err(e) = self.tx_output.send(certificate).await {
                    warn!("Failed to output certificate: {}", e);
                }
            }
        }
    }

    /// Returns the certificate (and the certificate's digest) originated by the leader of the
    /// specified round (if any).
    fn leader<'a>(
        &self,
        round: Round,
        coin_round: Round,
        dag: &'a Dag,
        coin_cache: &CoinCache,
    ) -> Option<&'a (Digest, Certificate)> {
        let by_round = dag.get(&round)?;

        // We elect the leader of round r-2 using either:
        // - round-robin (deterministic fallback), or
        // - a reproducible common-coin value derived from round-r certificates.
        let leader = match self.consensus_protocol {
            ConsensusProtocol::RoundRobin => {
                let coin = {
                    #[cfg(test)]
                    {
                        0
                    }
                    #[cfg(not(test))]
                    {
                        round
                    }
                };
                let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
                keys.sort();
                keys[coin as usize % self.committee.size()]
            }
            ConsensusProtocol::CommonCoin => {
                let coin = *coin_cache.get(&coin_round)?;
                let mut keys: Vec<_> = by_round.keys().cloned().collect();
                if keys.is_empty() {
                    return None;
                }
                keys.sort();
                keys[coin as usize % keys.len()]
            }
        };

        // Return its certificate and the certificate's digest.
        by_round.get(&leader)
    }

    fn common_coin_input(&self, round: Round, dag: &Dag) -> Option<CoinComputationInput> {
        let certificates = dag.get(&round)?;
        let quorum = self.committee.quorum_threshold() as usize;
        let threshold = coin_threshold(self.committee.size());

        let mut certificates: Vec<&Certificate> =
            certificates.values().map(|(_, certificate)| certificate).collect();
        certificates.sort_by_key(|certificate| certificate.digest());
        if certificates.len() < quorum {
            return None;
        }

        let authorities: Vec<PublicKey> = self.committee.authorities.keys().cloned().collect();
        let mut shares = Vec::new();
        for certificate in certificates.into_iter().take(quorum) {
            if certificate.header.coin_share.is_empty() {
                continue;
            }
            shares.push((
                certificate.origin(),
                certificate.header.coin_share.clone(),
            ));
        }

        Some(CoinComputationInput {
            round,
            authorities,
            threshold,
            shares,
        })
    }

    fn schedule_common_coin(
        &self,
        round: Round,
        dag: &Dag,
        pending_coin_rounds: &mut HashSet<Round>,
        coin_cache: &CoinCache,
        coin_result_tx: &UnboundedSender<(Round, Option<Round>)>,
    ) {
        if !matches!(self.consensus_protocol, ConsensusProtocol::CommonCoin) {
            return;
        }
        if coin_cache.contains_key(&round) || pending_coin_rounds.contains(&round) {
            return;
        }
        let input = match self.common_coin_input(round, dag) {
            Some(input) => input,
            None => return,
        };
        pending_coin_rounds.insert(round);
        let tx = coin_result_tx.clone();
        tokio::task::spawn_blocking(move || {
            let coin = recover_coin(
                &input.authorities,
                input.threshold,
                input.round,
                &input.shares,
            );
            let _ = tx.send((input.round, coin));
        });
    }

    fn round_has_quorum(&self, round: Round, dag: &Dag) -> bool {
        let Some(certificates) = dag.get(&round) else {
            return false;
        };
        let weight: Stake = certificates
            .values()
            .map(|(_, certificate)| self.committee.stake(&certificate.origin()))
            .sum();
        weight >= self.committee.quorum_threshold()
    }

    fn certificate_by_author<'a>(
        &self,
        round: Round,
        author: PublicKey,
        dag: &'a Dag,
    ) -> Option<&'a Certificate> {
        dag.get(&round)
            .and_then(|by_authority| by_authority.get(&author))
            .map(|(_, certificate)| certificate)
    }

    fn embedded_qc_links(
        &self,
        child: &Certificate,
        parent: &Certificate,
        commit_round: Round,
    ) -> bool {
        let Some(qc) = child.header.qc.as_ref() else {
            return false;
        };
        qc.target == parent.header.id
            && qc.round == parent.round()
            && qc.round < commit_round
            && qc.votes.iter().all(|vote| vote.voter_round < commit_round)
    }

    /// Flatten the dag referenced by the input certificate. This is a classic depth-first search (pre-order):
    /// https://en.wikipedia.org/wiki/Tree_traversal#Pre-order
    fn order_dag(&self, leader: &Certificate, state: &State) -> Vec<Certificate> {
        debug!("Processing sub-dag of {:?}", leader);
        let mut ordered = Vec::new();
        let mut already_ordered = HashSet::new();

        let mut buffer = vec![leader];
        while let Some(x) = buffer.pop() {
            debug!("Sequencing {:?}", x);
            ordered.push(x.clone());
            for parent in &x.header.parents {
                let (digest, certificate) = match state
                    .dag
                    .get(&(x.round() - 1))
                    .map(|x| x.values().find(|(x, _)| x == parent))
                    .flatten()
                {
                    Some(x) => x,
                    None => continue, // We already ordered or GC up to here.
                };

                // We skip the certificate if we (1) already processed it or (2) we reached a round that we already
                // committed for this authority.
                let mut skip = already_ordered.contains(&digest);
                skip |= state
                    .last_committed
                    .get(&certificate.origin())
                    .map_or_else(|| false, |r| r == &certificate.round());
                if !skip {
                    buffer.push(certificate);
                    already_ordered.insert(digest);
                }
            }
        }

        // Ensure we do not commit garbage collected certificates.
        ordered.retain(|x| x.round() + self.gc_depth >= state.last_committed_round);

        // Ordering the output by round is not really necessary but it makes the commit sequence prettier.
        ordered.sort_by_key(|x| x.round());
        ordered
    }
}
