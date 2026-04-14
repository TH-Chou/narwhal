# Copyright(C) Facebook, Inc. and its affiliates.
from fabric import task

from benchmark.local import LocalBench
from benchmark.logs import ParseError, LogParser
from benchmark.utils import Print
from benchmark.plot import Ploter, PlotError
from benchmark.instance import InstanceManager
from benchmark.remote import Bench, BenchError


@task
def local(ctx, debug=True, protocol='round_robin'):
    ''' Run benchmarks on localhost '''
    if protocol not in ('round_robin', 'common_coin'):
        raise BenchError('Invalid protocol: must be round_robin or common_coin')

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
        'consensus_protocol': protocol,
    }
    try:
        ret = LocalBench(bench_params, node_params).run(debug)
        print(ret.result())
    except BenchError as e:
        Print.error(e)


@task
def create(ctx, nodes=2):
    ''' Create a testbed'''
    try:
        InstanceManager.make().create_instances(nodes)
    except BenchError as e:
        Print.error(e)


@task
def destroy(ctx):
    ''' Destroy the testbed '''
    try:
        InstanceManager.make().terminate_instances()
    except BenchError as e:
        Print.error(e)


@task
def start(ctx, max=2):
    ''' Start at most `max` machines per data center '''
    try:
        InstanceManager.make().start_instances(max)
    except BenchError as e:
        Print.error(e)


@task
def stop(ctx):
    ''' Stop all machines '''
    try:
        InstanceManager.make().stop_instances()
    except BenchError as e:
        Print.error(e)


@task
def info(ctx):
    ''' Display connect information about all the available machines '''
    try:
        InstanceManager.make().print_info()
    except BenchError as e:
        Print.error(e)


@task
def install(ctx):
    ''' Install the codebase on all machines '''
    try:
        Bench(ctx).install()
    except BenchError as e:
        Print.error(e)


@task
def remote(
    ctx,
    debug=False,
    protocol='round_robin',
    faults=3,
    nodes=10,
    workers=1,
    rate=10_000,
    tx_size=512,
    duration=300,
    runs=2,
):
    ''' Run benchmarks on AWS '''
    if protocol not in ('round_robin', 'common_coin'):
        raise BenchError('Invalid protocol: must be round_robin or common_coin')

    bench_params = {
        'faults': int(faults),
        'nodes': [int(nodes)],
        'workers': int(workers),
        'collocate': True,
        'rate': [int(rate)],
        'tx_size': int(tx_size),
        'duration': int(duration),
        'runs': int(runs),
    }
    node_params = {
        'header_size': 1_000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200,  # ms
        'consensus_protocol': protocol,
    }
    try:
        Bench(ctx).run(bench_params, node_params, debug)
    except BenchError as e:
        Print.error(e)


@task
def remote_run_batch(
    ctx,
    debug=False,
    protocol='round_robin',
    batch_id='default',
    faults=3,
    nodes=10,
    workers=1,
    rates='10000',
    tx_size=512,
    duration=300,
    runs=2,
):
    ''' Run benchmarks on AWS and keep logs on remote machines for later collection '''
    if protocol not in ('round_robin', 'common_coin'):
        raise BenchError('Invalid protocol: must be round_robin or common_coin')

    rate_values = [int(x.strip()) for x in str(rates).split(',') if x.strip()]
    if not rate_values:
        raise BenchError('Invalid rates: provide comma-separated integers')

    bench_params = {
        'faults': int(faults),
        'nodes': [int(nodes)],
        'workers': int(workers),
        'collocate': True,
        'rate': rate_values,
        'tx_size': int(tx_size),
        'duration': int(duration),
        'runs': int(runs),
    }
    node_params = {
        'header_size': 1_000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200,  # ms
        'consensus_protocol': protocol,
    }
    try:
        Bench(ctx).run_batch(bench_params, node_params, str(batch_id), debug)
    except BenchError as e:
        Print.error(e)


@task
def remote_collect_batch(ctx, batch_id='default', out_dir='batch_downloads'):
    ''' Download archived batch logs from all AWS machines in one shot '''
    try:
        Bench(ctx).collect_batch(str(batch_id), str(out_dir))
    except BenchError as e:
        Print.error(e)


@task
def plot(ctx):
    ''' Plot performance using the logs generated by "fab remote" '''
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
