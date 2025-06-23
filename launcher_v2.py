import argparse
import os
import beelib
import load_dotenv
import datetime
import warnings

import pytz

from harmonizers import starter_job, processor_job
import logging
from pythonjsonlogger import jsonlogger

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter('%(asctime)s - %(levelname)s - %(name)s: %(message)s')
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
warnings.filterwarnings("ignore")

FREQ_CONFIG = {
    "PT15M": {"freq": "PT15M", 'days_to_gather': 7, 'days_to_overlap': 0, "gap_check": 3600 * 2},
    "PT1H": {"freq": "PT1H", 'days_to_gather': 7, 'days_to_overlap': 0, "gap_check": 3600 * 2},
    "P1D": {"freq": "P1D", 'days_to_gather': 60, 'days_to_overlap': 0, "gap_check": None},
    "P1W": {"freq": "P1W", 'days_to_gather': 60, 'days_to_overlap': 0, "gap_check": None},
    "P1M": {"freq": "P1M", 'days_to_gather': 180, 'days_to_overlap': 0, "gap_check": None}
}


if __name__ == '__main__':
    load_dotenv.load_dotenv()
    config = beelib.beeconfig.read_config()
    ap = argparse.ArgumentParser()
    ap.add_argument('--launcher', '-l', required=True, choices=['start', 'processor'])
    ap.add_argument('--diff', '-d', required=False, default="")
    ap.add_argument("--actions", "-a", required=False, nargs="+", default=["raw", "processors", "calculations", "limits"])
    ap.add_argument("--topic", "-t", required=False, default="icatprod.druid")
    ap.add_argument('--frequency', '-f', required=True)
    ap.add_argument('--start', '-s', required=False, default=None)
    ap.add_argument('--stop', '-p', required=False, default=None)
    ap.add_argument('--processors', '-n', required=False, default=4)

    if (os.getenv("PYCHARM_HOSTED_IGNORE") is None or os.getenv("PYCHARM_HOSTED_IGNORE") == 0) and os.getenv("PYCHARM_HOSTED") is not None:
        args = ap.parse_args(["-l", "processor", "-f", "PT15M",  "-n", "10", "-s",
                  "2024-11-01T00:00:00+00:00", "-p", "2024-12-10T00:00:00+00:00", "-d", "job-influx",
                  "-t", "icat.influx", "-a", "raw", "processors"])
        print(args)
    else:
        args = ap.parse_args()
        if args.stop is not None:
            row_stop = datetime.datetime.fromisoformat(args.stop)
        else:
            row_stop = datetime.datetime.now(pytz.timezone("Europe/Madrid"))
        if args.start is not None:
            row_start = datetime.datetime.fromisoformat(args.start)
        else:
            row_start = row_stop - datetime.timedelta(days=FREQ_CONFIG[args.frequency]['days_to_gather'])

        if args.launcher == 'start':
            starter_job(neo4j_connection=config['neo4j'], redis_connection=config['redis']['connection'],
                        freq=FREQ_CONFIG[args.frequency], diff=args.diff, num_processors=int(args.processors),
                        actions=args.actions
                        )
        elif args.launcher == 'processor':
            processor_job(redis_connection=config['redis']['connection'], hbase_connection=config['hbase'],
                          kafka_connection=config['kafka']['connection'], neo4j_connection=config['neo4j'],
                          druid_connection=config['druid']['connection'],
                          druid_topic=args.topic, druid_datasource=config['druid']['datasource'],
                          influx_connection=config['influx'],
                          ts_ini=row_start, ts_end=row_stop, freq=FREQ_CONFIG[args.frequency], diff=args.diff,
                          num_processors=int(args.processors), actions=args.actions)

