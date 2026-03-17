// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::messages::{Certificate, Header, Vote};
use crate::primary::Round;
use config::{Committee, Stake};
use crypto::Hash as _;
use crypto::{Digest, PublicKey, Signature};
use std::collections::{HashMap, HashSet};
 
/// Aggregates votes for a particular header into a certificate.
pub struct VotesAggregator {
    weight: Stake,
    votes: Vec<(PublicKey, Signature)>,
    used: HashSet<PublicKey>,
}
 
impl VotesAggregator {
    pub fn new() -> Self {
        Self {
            weight: 0,
            votes: Vec::new(),
            used: HashSet::new(),
        }
    }
 
    pub fn append(
        &mut self,
        vote: Vote,
        committee: &Committee,
        header: &Header,
    ) -> DagResult<Option<Certificate>> {
        let author = vote.author;
 
        // Ensure it is the first time this authority votes.
        ensure!(self.used.insert(author), DagError::AuthorityReuse(author));
 
        self.votes.push((author, vote.signature));
        self.weight += committee.stake(&author);
        if self.weight >= committee.quorum_threshold() {
            self.weight = 0; // Ensures quorum is only reached once.
            return Ok(Some(Certificate {
                header: header.clone(),
                votes: self.votes.clone(),
            }));
        }
        Ok(None)
    }
}
 //这部分的更改主要是为了修改原有narwhal代码中收集2f+1个certificates为收集2f+1个wave certificates
 
struct RoundCertificates {
    certificates: Vec<(PublicKey, Digest)>,
    used: HashSet<PublicKey>,
}
 
impl RoundCertificates {
    fn new() -> Self {
        Self {
            certificates: Vec::new(),
            used: HashSet::new(),
        }
    }
}
 
struct AuthorityRounds {
    contiguous_round: Round,
    received_rounds: HashSet<Round>,
}
 
impl AuthorityRounds {
    fn new() -> Self {
        Self {
            contiguous_round: 0,
            received_rounds: HashSet::new(),
        }
    }
 
    fn append(&mut self, round: Round) {
        if round <= self.contiguous_round {
            return;
        }
 
        if !self.received_rounds.insert(round) {
            return;
        }
 
        while self.received_rounds.remove(&(self.contiguous_round + 1)) {
            self.contiguous_round += 1;
        }
    }
}
 
/// Aggregate certificates with continuity from round 1: an authority counts for
/// round `r` only if we have observed all of its certificates from 1..=r.
pub struct ContinuousCertificatesAggregator {
    by_round: HashMap<Round, RoundCertificates>,
    by_authority: HashMap<PublicKey, AuthorityRounds>,
    last_emitted_round: Round,
}
 
impl ContinuousCertificatesAggregator {
    pub fn new() -> Self {
        Self {
            by_round: HashMap::new(),
            by_authority: HashMap::new(),
            last_emitted_round: 0,
        }
    }
 
    pub fn append(
        &mut self,
        certificate: Certificate,
        committee: &Committee,
    ) -> Vec<(Vec<Digest>, Round)> {
        let round = certificate.round();
        let origin = certificate.origin();
        let digest = certificate.digest();
 
        let per_round = self
            .by_round
            .entry(round)
            .or_insert_with(RoundCertificates::new);
        if per_round.used.insert(origin) {
            per_round.certificates.push((origin, digest));
            self.by_authority
                .entry(origin)
                .or_insert_with(AuthorityRounds::new)
                .append(round);
        }
 
        let mut ready = Vec::new();
        loop {
            let target_round = self.last_emitted_round + 1;
            let Some(round_certificates) = self.by_round.get(&target_round) else {
                break;
            };
 
            let mut weight = 0;
            let mut parents = Vec::new();
            for (name, digest) in &round_certificates.certificates {
                let contiguous_round = self
                    .by_authority
                    .get(name)
                    .map_or(0, |x| x.contiguous_round);
                if contiguous_round >= target_round {
                    weight += committee.stake(name);
                    parents.push(digest.clone());
                }
            }
 
            if weight < committee.quorum_threshold() {
                break;
            }
 
            self.last_emitted_round = target_round;
            self.by_round.remove(&target_round);
            ready.push((parents, target_round));
        }
 
        ready
    }
}
