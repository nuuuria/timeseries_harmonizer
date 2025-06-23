import os
import time
from datetime import datetime
from functools import reduce

import load_dotenv
import pandas as pd
import beelib
import neo4j
import tools.copy_tables.source_config as sources
import re
import happybase
from tools.plot_utils import plot_dataframes


def download_data(server, table, row, files, times, periods):
    for r, f, t, p in zip(row, files, times, periods):
        df_raw = pd.DataFrame()
        ts = int(datetime.fromisoformat(t).timestamp())
        if p == "-":
            row_start = f"{r}~"
            row_stop = f"{r}~{ts}"
        elif p == "+":
            row_start = f"{r}~{ts}"
            row_stop = f"{r[:len(r)-1] + chr(ord(r[-1]) + 1)}~"
        for data in beelib.beehbase.get_hbase_data_batch(server, table, row_start=row_start, row_stop=row_stop):
            items = []
            for key, row in data:
                item = {'row': key.decode("utf-8")}
                for k, v in row.items():
                    item[k.decode("utf-8")] = v.decode("utf")
                    items.append(item)
            df_raw = pd.concat([df_raw, pd.DataFrame(items)], ignore_index=True)
        df_raw.to_csv(f)


def plot_df(df):
    plot_dataframes([{"title":"a", "row": 1, "col":1, "data":[{"df":df, "fields": ["info:value"]}]}])


def delete_hbase_rows(server, table, devices, files, times, periods):
    for r, f, t, p in zip(devices, files, times, periods):
        df = pd.read_csv(f)
        to_delete_rows = [d['row'] for _, d in df.iterrows()]
        for i in range(0, len(to_delete_rows), 100):
            hb = happybase.Connection(**server)
            t = hb.table(table)
            b = t.batch()
            for ii in range(0, 100):
                try:
                    b.delete(to_delete_rows[i + ii])
                except IndexError:
                    break
            b.send()


def migrate_data(server, table, actions):
    for action in actions:
        print(f"moving from {action[0]} to {action[1]}...")
        s_file = folder + beelib.beetransformation.create_hash(action[0]) + "-source.csv"
        df_source = pd.read_csv(s_file)
        k_ts = df_source['row'].apply(lambda x: tuple(x.split("~")))
        df_source['row'] = k_ts.apply(lambda x: f"{action[1]}~{x[1]}")
        to_send = []
        data_rows = [tuple(x.split(":")) for x in df_source.columns[2:]]
        for _, t in df_source.iterrows():
            item = {"row": t['row']}
            for ns, c in data_rows:
                item[c] = t[f"{ns}:{c}"]
            to_send.append(item)
        d_rows_hbase = list(reduce(lambda acc, y: {**acc, y[0]: acc.get(y[0], []) + [y[1]]}, data_rows, {}).items())
        beelib.beehbase.save_to_hbase(to_send, table, server, d_rows_hbase, row_fields=['row'])


if __name__ == '__main__':
    """
    Before running the store data. Store all data to a file, if you don't want to loose the data.
    
    1. Extract data to a file for the desired devices
    2. Remove data from hbase for the desired devices and period
    3. Overwrite data to hbase on the desired device.
    # actions:
    [(source, destination, time, period),...]
    - source: the uri of the source device
    - destination, the uri of the destination device
    - time: the time to perform the change. 
    - period: + change after time - change before time
    """
    load_dotenv.load_dotenv()
    config = beelib.beeconfig.read_config()
    server = config['hbase']['hbase_infraestructures']['connection']
    table = "modbus:timeseries_id_"
    folder = "export/"
    actions = [
        (
            "https://icat.cat#measurement-SNH-09315-ixon-modbus-10.10.10.50-2-32000-PT15M",
            "https://icat.cat#measurement-SNH-09315-ixon-modbus-10.10.10.50-1-32000-PT15M",
            "2025-03-27 11:00:00+01:00", "-"
        ),
        (
            "https://icat.cat#measurement-SNH-09315-ixon-modbus-10.10.10.50-2-32080-PT15M",
            "https://icat.cat#measurement-SNH-09315-ixon-modbus-10.10.10.50-1-32080-PT15M",
            "2025-03-27 11:00:00+01:00", "-"
        ),
        (
            "https://icat.cat#measurement-SNH-09315-ixon-modbus-10.10.10.50-2-32087-PT15M",
            "https://icat.cat#measurement-SNH-09315-ixon-modbus-10.10.10.50-1-32087-PT15M",
            "2025-03-27 11:00:00+01:00", "-"
        ),
        (
            "https://icat.cat#measurement-SNH-09315-ixon-modbus-10.10.10.50-2-32106-PT15M",
            "https://icat.cat#measurement-SNH-09315-ixon-modbus-10.10.10.50-1-32106-PT15M",
            "2025-03-27 11:00:00+01:00", "-"
        ),
        (
            "https://icat.cat#measurement-SNH-09315-ixon-modbus-10.10.10.50-1-32000-PT15M",
            "https://icat.cat#measurement-SNH-09315-ixon-modbus-10.10.10.50-22-32000-PT15M",
            "2025-03-27 11:00:00+01:00", "-"
        ),
        (
            "https://icat.cat#measurement-SNH-09315-ixon-modbus-10.10.10.50-1-32080-PT15M",
            "https://icat.cat#measurement-SNH-09315-ixon-modbus-10.10.10.50-22-32080-PT15M",
            "2025-03-27 11:00:00+01:00", "-"
        ),
        (
            "https://icat.cat#measurement-SNH-09315-ixon-modbus-10.10.10.50-1-32087-PT15M",
            "https://icat.cat#measurement-SNH-09315-ixon-modbus-10.10.10.50-22-32087-PT15M",
            "2025-03-27 11:00:00+01:00", "-"
        ),
        (
            "https://icat.cat#measurement-SNH-09315-ixon-modbus-10.10.10.50-1-32106-PT15M",
            "https://icat.cat#measurement-SNH-09315-ixon-modbus-10.10.10.50-22-32106-PT15M",
            "2025-03-27 11:00:00+01:00", "-"
        ),

    ]

    if os.getenv("PYCHARM_HOSTED") is not None:
        pass
    else:
        devices = []
        files = []
        times = []
        periods = []
        os.makedirs(folder, exist_ok=True)
        for action in actions:
            if action[0]:
                devices.append(action[0])
                files.append(folder + beelib.beetransformation.create_hash(action[0]) + "-source.csv")
                times.append(action[2])
                periods.append(action[3])
            if action[1]:
                devices.append(action[1])
                files.append(folder + beelib.beetransformation.create_hash(action[1]) + "-destination.csv")
                times.append(action[2])
                periods.append(action[3])

        #download_data(server, table, devices, files, times, periods)
        delete_hbase_rows(server, table, devices, files, times, periods)
        migrate_data(server, table, actions)
