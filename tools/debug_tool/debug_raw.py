import datetime
import time
from urllib.parse import quote
import beelib
import load_dotenv
import neo4j
import numpy as np
import pandas as pd
import pytz
from harmonizers import harmonize_raw_data, get_raw_data
from harmonizers.sources.bacnet import bacnet
from harmonizers.sources.dexma import dexma
from harmonizers.sources.manttest import manttest, manttest_eco
from harmonizers.sources.modbus import modbus
from launcher_v2 import FREQ_CONFIG
from tools.plot_utils import plot_dataframes


harmonized_hashes = ["02ea462b0588b486bd92b84587de6b3c66c9b2ac4236cf1113447aac02e037da", "4a633d4c6ff22c37b566b3d64e5bf5ba9c2a0fa9e0594dbb8ff2f9cd9980ed66"]
sources = [dexma, modbus]#, manttest, manttest_eco]
post_processors = []
config_file = None
freq = FREQ_CONFIG["PT15M"]

#final i al principi
ts_ini = datetime.datetime.fromisoformat("2025-05-01T11:30:00+00:00")
ts_end = datetime.datetime.fromisoformat("2025-05-08T11:30:00+00:00")

ts_end = datetime.datetime.fromisoformat("2025-05-01T12:30:00+00:00")
ts_ini = ts_end - datetime.timedelta(days=7)

#ts_end = datetime.datetime.now(datetime.timezone.utc)
#ts_ini = ts_end - datetime.timedelta(days=freq['days_to_gather'])


load_dotenv.load_dotenv()
config = beelib.beeconfig.read_config(config_file)
driver = neo4j.GraphDatabase.driver(**config['neo4j'])

# GET RAW DEVICES BY SOURCE
raw_dev = {}
for i, s in enumerate(sources):
    source_devices = pd.DataFrame()
    for s_t in s['device_query']:
        try:
            for attempt in range(5):
                try:
                    devices = driver.session().run(s_t.format(freq=freq['freq'])).data()
                    break
                except Exception as e:
                    devices = []
                    pass
            if not devices:
                print("no devices")
                continue
            device_df = pd.json_normalize(devices).explode("raw_data")
            device_df = pd.concat([device_df.drop('raw_data', axis=1),
                                   device_df['raw_data'].apply(pd.Series).rename(
                                       columns={c: f"raw_data.{c}" for c in
                                                device_df['raw_data'].apply(pd.Series).columns})], axis=1)
            source_devices = pd.concat([source_devices, device_df])
        except KeyError as e:
            print(e)
            pass
    if source_devices.empty:
        continue
    raw_dev[i] = source_devices[source_devices['harmonized.bigg__hash'].isin(harmonized_hashes)]



# HARMONIZE RAW DATA BY SOURCE
raw_data = []
harm_data = []
harm_data_total = {}
for i, df_dev in raw_dev.items():
    source = sources[i]
    has_count = {}
    for _, dev in df_dev.iterrows():
        try:
            has_count[dev['harmonized.bigg__hash']] = has_count[dev['harmonized.bigg__hash']] + 1
        except KeyError as e:
            has_count[dev['harmonized.bigg__hash']] = 0
        r_data = get_raw_data(dev, source, config['hbase'], ts_ini, ts_end, freq)
        raw_data.append({"source": i, f"{dev['harmonized.bigg__hash']}-{has_count[dev['harmonized.bigg__hash']]}": r_data})
        s = time.time()
        data = harmonize_raw_data(dev, source, config['hbase'], ts_ini, ts_end, freq)
        print("time_harmonizer: ", time.time() - s)
        if data is None or data.empty:
            continue
        data = data.dropna(subset=['value'])
        data.index = data.index.tz_localize(pytz.UTC)
        harm_data.append({"source": i, dev['harmonized.bigg__hash']: data})
        s = time.time()
        df_device_final = data.copy(deep=True)
        df_device_final['property'] = (dev['harmonized.property'].
                                       replace("https://bigg-project.eu/ontology#", "").
                                       replace("https://saref.etsi.org/core/", ""))
        df_device_final['hash'] = dev['harmonized.bigg__hash']
        df_device_final['value'] = df_device_final['value'].round(5)
        df_to_save = df_device_final.reset_index().apply(
            beelib.beedruid.harmonize_for_druid, timestamp_key="timestamp", value_key="value",
            hash_key="hash",
            property_key="property", is_real=True, freq=freq['freq'],
            axis=1)
        print("time_preparation: ", time.time() - s)
        try:
            common_index = data["value"].index.union(harm_data_total[dev['harmonized.bigg__hash']]['value'].index)
            s1 = data["value"].reindex(common_index)
            harm_data_total[dev['harmonized.bigg__hash']] = harm_data_total[dev['harmonized.bigg__hash']].reindex(common_index)
            harm_data_total[dev['harmonized.bigg__hash']]['value'] = s1.combine_first(
                harm_data_total[dev['harmonized.bigg__hash']]['value'])

            harm_data_total[dev['harmonized.bigg__hash']] = harm_data_total[dev['harmonized.bigg__hash']].dropna(
                subset=["value"])
        except KeyError:
            harm_data_total[dev['harmonized.bigg__hash']] = data

#POST-PROCESS


# # plot sources and harmonized
# for hash_, data in harm_data_total.items():
#     df_plots = []
#     raw = [x for x in raw_data if list(x.keys())[1].startswith(hash_)]
#     harm = [x for x in harm_data if hash_ in x.keys()]
#     row_chart = 1
#     for i, hh in enumerate(harm):
#         sou = hh['source']
#         ii=0
#         for r in [x for x in raw if x['source'] == sou]:
#             raw_df_plot = raw[i+ii][f"{hash_}-{ii}"]
#             if not raw_df_plot.empty:
#                 df_plots.append({"title": f"source:{sources[hh['source']]['name']}-{ii}", "row": row_chart, "col": 1,
#                                  "data": {"df": raw_df_plot, "fields": ["value"]}})
#             else:
#                 df_plots.append({"title": f"source:{sources[hh['source']]['name']}-{ii}", "row": row_chart, "col": 1,
#                                  "data": {"df": pd.DataFrame({"value": [np.nan]}), "fields": ["value"]}})
#             row_chart += 1
#             ii += 1
#
#         df_plots.append({"title": f"source:{sources[hh['source']]['name']}", "row": row_chart, "col": 1,
#                          "data": {"df": hh[hash_], "fields": ["value"]}})
#         row_chart += 1
#
#     df_plots.append({"title": f"Harmonized:{hash_}", "row": row_chart, "col": 1,
#                      "data": {"df": data, "fields": ["value"]}})
#     plot_dataframes(df_plots)
