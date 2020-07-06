import argparse
import asyncio
import os
import threading

from aiohttp import web

from . import config
from . import hsds_logger as log


async def start_app_runner(runner, address, port):
    """Start AppRunner.

    :param runner: Runner to serve from thread
    :param str address: See :class:`aiohttp.web.TCPSite`
    :param int port: See :class:`aiohttp.web.TCPSite`
    """
    await runner.setup()
    site = web.TCPSite(runner, address, port)
    await site.start()


def run_runner_in_thread(runner, address, port):
    """Run a runner in a thread.

    :param runner: Runner to serve from thread
    :param str address: See :class:`aiohttp.web.TCPSite`
    :param int port: See :class:`aiohttp.web.TCPSite`
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(runner.setup())
    site = web.TCPSite(runner, address, port)
    loop.run_until_complete(site.start())
    loop.run_forever()
    loop.run_until_complete(runner.cleanup())  # TODO how to close?


_HELP_USAGE = "Starts hsds a REST-based service for HDF5 data."

_HELP_EPILOG = """Examples:

- with openio/sds data storage:

  hsds --s3-gateway http://localhost:6007 --access-key-id demo:demo --secret-access-key DEMO_PASS --password-file ./admin/config/passwd.txt --bucket-name hsds.test

- with a POSIX-based storage for 'hsds.test' sub-folder in the './data' folder:

  hsds --bucket-dir ./data/hsds.test
"""


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        usage=_HELP_USAGE,
        epilog=_HELP_EPILOG)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--bucket-dir', nargs=1, type=str, dest='bucket_dir',
        help='Directory where to store the object store data')
    group.add_argument(
        '--bucket-name', nargs=1, type=str, dest='bucket_name',
        help='Name of the bucket to use (e.g., "hsds.test").')

    parser.add_argument('--host', nargs=1, default=['localhost'],
        type=str, dest='host',
        help="Address the service node is bounds with (default: localhost).")
    parser.add_argument('-p', '--port', nargs=1, default=config.get('sn_port'),
        type=int, dest='port',
        help='Service node port (default: %d).' % config.get('sn_port'))

    parser.add_argument(
        '--s3-gateway', nargs=1, type=str, dest='s3_gateway',
        help='S3 service endpoint (e.g., "http://openio:6007")')
    parser.add_argument(
        '--access-key-id', nargs=1, type=str, dest='access_key_id',
        help='s3 access key id (e.g., "demo:demo")')
    parser.add_argument(
        '--secret-access-key', nargs=1, type=str, dest='secret_access_key',
        help='s3 secret access key (e.g., "DEMO_PASS")')

    parser.add_argument(
        '--password-file', nargs=1, default=[''], type=str, dest='password_file',
        help="Path to file containing authentication passwords (default: No authentication)")

    args, extra_args = parser.parse_known_args()

    config.cfg['standalone_app'] = 'True'
    #config.cfg['head_endpoint'] = 'http://localhost:' + str(config.get('head_port'))

    address = '%s:%d' % (args.host, args.port)
    #config.cfg['sn_port'] = str(args.port)
    #config.cfg['hsds_endpoint'] = 'http://' + address
    config.cfg['public_dns'] = address

    config.cfg['password_file'] = args.password_file[0]
    if args.s3_gateway is not None:
        config.cfg['aws_s3_gateway'] = args.s3_gateway[0]
    if args.secret_access_key is not None:
        config.cfg['aws_secret_access_key'] = args.secret_access_key[0]
    if args.access_key_id is not None:
        config.cfg['aws_access_key_id'] = args.access_key_id[0]

    if args.bucket_dir is not None:
        directory = os.path.abspath(args.bucket_dir[0])
        config.cfg['bucket_name'] = os.path.basename(directory)
        config.cfg['root_dir'] = os.path.dirname(directory)
    else:
        config.cfg['bucket_name'] = args.bucket_name[0]
        config.cfg['root_dir'] = ''

    # Start apps

    from . import datanode, servicenode, headnode

    loop = asyncio.get_event_loop()

    log.info("Creating runners")

    head_runner = web.AppRunner(headnode.create_app(loop))

    log.info('Runners created')

    loop.run_until_complete(start_app_runner(
        head_runner, 'localhost', config.get('head_port')))
    runners = [head_runner]

    # Starts DN and SN in threads
    dn_threads = []
    for index in range(int(config.get('target_dn_count'))):
        dn_runner = web.AppRunner(datanode.create_app(loop))
        runners.append(dn_runner)
        dn_threads.append(threading.Thread(
            target=run_runner_in_thread,
            args=(dn_runner, 'localhost', config.get('dn_port') + index)))
        dn_threads[-1].start()

    sn_threads = []
    for index in range(int(config.get('target_sn_count'))):
        sn_runner = web.AppRunner(servicenode.create_app(loop))
        runners.append(sn_runner)
        sn_threads.append(threading.Thread(
            target=run_runner_in_thread,
            args=(sn_runner, 'localhost', config.get('sn_port') + index)))
        sn_threads[-1].start()

    log.info('Loop about to start')

    try:
        loop.run_forever()
    except:
        pass
    finally:
        log.info('Runners about to stop')

        for runner in reversed(runners):
            loop.run_until_complete(runner.cleanup())
   
