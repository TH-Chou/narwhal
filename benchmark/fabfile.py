# Copyright(C) Facebook, Inc. and its affiliates.
import csv
from fabric import task

from benchmark.local import LocalBench
from benchmark.logs import ParseError, LogParser
from benchmark.utils import Print, BenchError


@task
def local(ctx, debug=True):
    ''' Run benchmarks on localhost '''
    bench_params = {
        'faults': 0,
        'nodes': 4,
        'workers': 1,
        'rate': 50_000,
        'tx_size': 512,
        'duration': 20,
    }
    node_params = {
        'header_size': 1_000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200,  # ms
        'consensus_protocol': 'round_robin',
    }
    try:
        ret = LocalBench(bench_params, node_params).run(debug)
        print(ret.result())
    except BenchError as e:
        Print.error(e)


@task
def compare_consensus(ctx, duration=60, debug=True):
    ''' Compare round_robin vs common_coin on localhost '''
    bench_params = {
        'faults': 0,
        'nodes': 4,
        'workers': 1,
        'rate': 50_000,
        'tx_size': 512,
        'duration': int(duration),
    }
    node_params = {
        'header_size': 1_000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200,  # ms
    }

    try:
        protocols = ['round_robin', 'common_coin']
        results = {}
        for protocol in protocols:
            Print.heading(
                f'Running local benchmark with protocol={protocol} '
                f'for {bench_params["duration"]}s'
            )
            current = dict(node_params)
            current['consensus_protocol'] = protocol
            parser = LocalBench(bench_params, current).run(debug)
            print(parser.result())
            results[protocol] = parser.metrics()

        Print.heading('Consensus protocol comparison')
        print('-----------------------------------------')
        for protocol in protocols:
            metrics = results[protocol]
            print(
                f'{protocol}: '
                f'consensus_tps={round(metrics["consensus_tps"]):,} tx/s, '
                f'consensus_latency={round(metrics["consensus_latency_ms"]):,} ms, '
                f'end_to_end_tps={round(metrics["end_to_end_tps"]):,} tx/s, '
                f'end_to_end_latency={round(metrics["end_to_end_latency_ms"]):,} ms'
            )
        print('-----------------------------------------')

    except BenchError as e:
        Print.error(e)


@task
def compare_consensus_groups(
    ctx,
    duration=60,
    debug=True,
    rate=50_000,
    rounds=10,
    output_csv='results/consensus_fault_comparison_avg.csv',
):
    ''' Compare protocols on grouped faults with 16 nodes (faults=0,1,2,3), averaged over rounds '''
    node_params = {
        'header_size': 1_000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200,  # ms
    }

    protocols = ['round_robin', 'common_coin']
    faults_group = [0, 1, 2, 3]
    results = {protocol: {} for protocol in protocols}
    rounds = int(rounds)
    if rounds <= 0:
        raise BenchError('rounds must be greater than 0')

    try:
        for fault in faults_group:
            bench_params = {
                'faults': fault,
                'nodes': 16,
                'workers': 1,
                'rate': int(rate),
                'tx_size': 512,
                'duration': int(duration),
            }
            for protocol in protocols:
                metrics_runs = []
                for run in range(rounds):
                    Print.heading(
                        f'Run {run + 1}/{rounds} | nodes=16 faults={fault} '
                        f'protocol={protocol} duration={bench_params["duration"]}s'
                    )
                    current = dict(node_params)
                    current['consensus_protocol'] = protocol
                    parser = LocalBench(bench_params, current).run(debug)
                    print(parser.result())
                    metrics_runs.append(parser.metrics())

                results[protocol][fault] = {
                    'consensus_protocol': protocol,
                    'consensus_tps': sum(
                        x['consensus_tps'] for x in metrics_runs
                    ) / rounds,
                    'consensus_latency_ms': sum(
                        x['consensus_latency_ms'] for x in metrics_runs
                    ) / rounds,
                    'end_to_end_tps': sum(
                        x['end_to_end_tps'] for x in metrics_runs
                    ) / rounds,
                    'end_to_end_latency_ms': sum(
                        x['end_to_end_latency_ms'] for x in metrics_runs
                    ) / rounds,
                }

        Print.heading(
            f'Grouped consensus comparison (nodes=16, averaged over {rounds} runs)'
        )
        print('------------------------------------------------------------')
        for fault in faults_group:
            for protocol in protocols:
                metrics = results[protocol][fault]
                print(
                    f'faults={fault}, protocol={protocol}: '
                    f'consensus_tps={round(metrics["consensus_tps"]):,} tx/s, '
                    f'consensus_latency={round(metrics["consensus_latency_ms"]):,} ms, '
                    f'end_to_end_tps={round(metrics["end_to_end_tps"]):,} tx/s, '
                    f'end_to_end_latency={round(metrics["end_to_end_latency_ms"]):,} ms'
                )
        print('------------------------------------------------------------')

        with open(output_csv, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    'fault',
                    'protocol',
                    'consensus_tps',
                    'consensus_latency_ms',
                    'end_to_end_tps',
                    'end_to_end_latency_ms',
                    'rounds',
                ]
            )
            for fault in faults_group:
                for protocol in protocols:
                    metrics = results[protocol][fault]
                    writer.writerow(
                        [
                            fault,
                            protocol,
                            f'{metrics["consensus_tps"]:.6f}',
                            f'{metrics["consensus_latency_ms"]:.6f}',
                            f'{metrics["end_to_end_tps"]:.6f}',
                            f'{metrics["end_to_end_latency_ms"]:.6f}',
                            rounds,
                        ]
                    )
        print(f'Averaged metrics exported to {output_csv}')

    except BenchError as e:
        Print.error(e)


@task
def create(ctx, nodes=2):
    ''' Create a testbed'''
    from benchmark.instance import InstanceManager

    try:
        InstanceManager.make().create_instances(nodes)
    except BenchError as e:
        Print.error(e)


@task
def destroy(ctx):
    ''' Destroy the testbed '''
    from benchmark.instance import InstanceManager

    try:
        InstanceManager.make().terminate_instances()
    except BenchError as e:
        Print.error(e)


@task
def start(ctx, max=2):
    ''' Start at most `max` machines per data center '''
    from benchmark.instance import InstanceManager

    try:
        InstanceManager.make().start_instances(max)
    except BenchError as e:
        Print.error(e)


@task
def stop(ctx):
    ''' Stop all machines '''
    from benchmark.instance import InstanceManager

    try:
        InstanceManager.make().stop_instances()
    except BenchError as e:
        Print.error(e)


@task
def info(ctx):
    ''' Display connect information about all the available machines '''
    from benchmark.instance import InstanceManager

    try:
        InstanceManager.make().print_info()
    except BenchError as e:
        Print.error(e)


@task
def install(ctx):
    ''' Install the codebase on all machines '''
    from benchmark.remote import Bench

    try:
        Bench(ctx).install()
    except BenchError as e:
        Print.error(e)


@task
def remote(ctx, debug=False):
    ''' Run benchmarks on AWS '''
    from benchmark.remote import Bench

    bench_params = {
        'faults': 3,
        'nodes': [10],
        'workers': 1,
        'collocate': True,
        'rate': [10_000, 110_000],
        'tx_size': 512,
        'duration': 300,
        'runs': 2,
    }
    node_params = {
        'header_size': 1_000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200,  # ms
        'consensus_protocol': 'round_robin',
    }
    try:
        Bench(ctx).run(bench_params, node_params, debug)
    except BenchError as e:
        Print.error(e)


@task
def plot(ctx):
    ''' Plot performance using the logs generated by "fab remote" '''
    from benchmark.plot import Ploter, PlotError

    plot_params = {
        'faults': [0],
        'nodes': [10, 20, 50],
        'workers': [1],
        'collocate': True,
        'tx_size': 512,
        'max_latency': [3_500, 4_500]
    }
    try:
        Ploter.plot(plot_params)
    except PlotError as e:
        Print.error(BenchError('Failed to plot performance', e))


@task
def kill(ctx):
    ''' Stop execution on all machines '''
    from benchmark.remote import Bench

    try:
        Bench(ctx).kill()
    except BenchError as e:
        Print.error(e)


@task
def logs(ctx):
    ''' Print a summary of the logs '''
    try:
        print(LogParser.process('./logs', faults='?').result())
    except ParseError as e:
        Print.error(BenchError('Failed to parse logs', e))
