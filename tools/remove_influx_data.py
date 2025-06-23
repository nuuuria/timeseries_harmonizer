import datetime
import os
import beelib
import argparse
import pandas as pd
from dotenv import load_dotenv
from lib2 import send_to_kafka


def remove_data_from_influx(freq, start, stop, hash, property, real, config, topic):
    print(freq, start, stop, hash, property, real, config, topic)
    config = beelib.beeconfig.read_config(config)
    index = pd.date_range(pd.Timestamp(start).floor(pd.Timedelta(freq)), pd.Timestamp(stop).floor(pd.Timedelta(freq)), freq=pd.Timedelta(freq))
    df = pd.DataFrame(data={"value": None, "dev_hash": hash, "property": property, "isReal": real},
                      index=index)
    df.reset_index(names=["timestamp"], inplace=True)
    to_send = df.apply(beelib.beedruid.harmonize_for_druid, args=("timestamp", "value", "dev_hash",
                                                                  "property", real, freq), axis=1)
    druid_producer = beelib.beekafka.create_kafka_producer(config['kafka']['connection'], encoding="JSON")
    data = send_to_kafka(druid_producer, topic, to_send)
    print(data)


def boolean(v):
    if v in ['True', "False"]:
        return True if v == "True" else False
    else:
        raise ValueError(f"{v} is not a boolean")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument('--freq', '-f', required=True,
                    type=str)
    ap.add_argument('--start', '-s', required=True,
                    default=datetime.datetime.fromisoformat("2024-01-01T00:00:00+00:00"),
                    type=datetime.datetime.fromisoformat)
    ap.add_argument('--stop', '-p', required=True,
                    default=datetime.datetime.now(tz=datetime.timezone.utc), type=datetime.datetime.fromisoformat)
    ap.add_argument('--hash', required=True, type=str)
    ap.add_argument('--property', required=True, type=str)
    ap.add_argument('--real', required=True, type=boolean)
    ap.add_argument('--config', required=False, default=None, type=str)
    ap.add_argument("--topic", "-t", required=False, default="icat.influx", type=str)

    load_dotenv()
    if os.getenv("PYCHARM_HOSTED") is not None:
        print("PYCHARM_HOSTED")
        args = ap.parse_args(["-f", "PT15M", "-s", "2025-03-04T13:00:00+01:00", "-p", "2025-03-04T15:00:00+01:00",
                              "--hash", "7f8db3401e144f04f2404f7c07dd7beec25d5d253e3e6b50049fa12ef96265b6", "--property", "Energy.Active", "--real", "True"])
    else:
        args = ap.parse_args()
        remove_data_from_influx(**vars(args))
