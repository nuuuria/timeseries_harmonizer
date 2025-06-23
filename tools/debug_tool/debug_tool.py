import datetime
import re
import time
from urllib.parse import quote

import beelib
import load_dotenv
import neo4j
import pandas as pd
import pytz
import xml.etree.ElementTree as ElementTree


from harmonizers import harmonize_raw_data, PVProcessor, get_raw_data
from harmonizers.sources.dexma import dexma
from harmonizers.sources.modbus import modbus
from launcher_v2 import FREQ_CONFIG
from lib2.calculate_formulas import CalculateFunctions
from tools.plot_utils import plot_dataframes

ts_ini = datetime.datetime.fromisoformat("2025-02-28T00:00:00+00:00")
ts_end = datetime.datetime.fromisoformat("2025-03-03T10:00:00+00:00")
patrimony_id = "INC-03366"
harmonized_hashes = []
calculation_hashes = ["5f144351b14b7cbc19bbca16993fd9a90ea13745a417a750a4d11f58464e59cc"]
sources = [dexma, modbus]
post_processors = []
config_file = None
freq = FREQ_CONFIG["PT15M"]


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
                    pass
            device_df = pd.json_normalize(devices).explode("raw_data")
            device_df = pd.concat([device_df.drop('raw_data', axis=1),
                                   device_df['raw_data'].apply(pd.Series).rename(
                                       columns={c: f"raw_data.{c}" for c in
                                                device_df['raw_data'].apply(pd.Series).columns})], axis=1)
            source_devices = pd.concat([source_devices, device_df])
        except KeyError:
            pass
    if source_devices.empty:
        continue
    raw_dev[i] = source_devices[source_devices['raw_data.uri'].apply(lambda x: True if quote(patrimony_id) in x else False)]
    raw_dev[i] = raw_dev[i][raw_dev[i]['harmonized.bigg__hash'].isin(harmonized_hashes)]

# HARMONIZE RAW DATA BY SOURCE
raw_data = []
harm_data = []
harm_data_total = {}
for i, df_dev in raw_dev.items():
    source = sources[i]
    for _, dev in df_dev.iterrows():
        r_data = get_raw_data(dev, source, config['hbase'], ts_ini, ts_end, freq)
        raw_data.append({"source": i, dev['harmonized.bigg__hash']: r_data})
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
# plot sources and harmonized
for hash_, data in harm_data_total.items():
    df_plots = []
    raw = [x for x in raw_data if hash_ in x.keys()]
    harm = [x for x in harm_data if hash_ in x.keys()]
    for i, hh in enumerate(harm):
        df_plots.append({"title": f"source:{sources[hh['source']]['name']}", "row": i*2+1, "col": 1,
                         "data": {"df": raw[i][hash_], "fields": ["value"]}})
        df_plots.append({"title": f"source:{sources[hh['source']]['name']}", "row": i*2+2, "col": 1,
                         "data": {"df": hh[hash_], "fields": ["value"]}})

    df_plots.append({"title": f"Harmonized:{hash_}", "row": len(harm)*2 + 1, "col": 1,
                     "data": {"df": data, "fields": ["value"]}})
    plot_dataframes(df_plots)

# get harmonized data from influx before running post processors
# ==========================
# influx_harm = {}
# for hash_ in harm_data_total.keys():
#     influx_harm[hash_] = beelib.beeinflux.get_timeseries_by_hash(hash_,
#                                                                  "PT15M", config['influx'], ts_ini, ts_end)
# # COMPARE HARM WITH INFLUX
# for hash_ in harm_data_total.keys():
#     comp1 = harm_data_total[hash_][['value']]
#     comp1.index = comp1.index.tz_localize(pytz.UTC)
#     comp1['value'] = comp1.value.round(2)
#     comp2 = influx_harm[hash_][['value']]
#     comp2.index=comp2.index.set_names("timestamp")
#     comp2['value'] = comp2.value.round(2)
#     try:
#         c = comp1.compare(comp2)
#         if not c.empty:
#             raise Exception("different")
#     except Exception as e:
#         df_plots = [
#             {"title": f"Harmonized:{hash_}", "row": 1, "col": 1,
#              "data": [{"df": comp1, "fields": [{"field": "value", "color": "blue", "title": "here"}]},
#                       {"df": comp2, "fields": [{"field": "value", "color": "red", "title": "influx"}]}
#                       ]
#              }
#         ]
#         plot_dataframes(df_plots)
#
#
# ==========================
# GET POST PROCESSORS DEVICES
post_dev = {}
for i, post in enumerate(post_processors):
    query = post.get_devices(freq)
    devices = driver.session().run(query).data()
    if not devices:
        continue
    devices = pd.DataFrame(devices)
    post_dev[i] = devices[devices['patrimony'] == patrimony_id]

# APPLY POST PROCESSORS
post_harm = harm_data_total.copy()
for i, post_devices in post_dev.items():
    post = post_processors[i]
    for _, devs in post_devices.iterrows():
        gen_hash = [x['hash'] for x in devs['devices'] if x['name'] == "Generation"][0]
        exp_hash = [x['hash'] for x in devs['devices'] if x['name'] == "ExportedToGrid"][0]
        update, post_df = post._apply_processor(post_harm[gen_hash].copy(deep=True),
                                                post_harm[exp_hash].copy(deep=True))
        if update:
            post_harm[exp_hash] = post_df

# POST PROCESS PLOT
df_plots = []
for i, hash_ in enumerate(harm_data_total.keys()):
    df_total = harm_data_total[hash_][['value']]
    df_total['v'] = post_harm[hash_][['value']].astype(float)
    df_plots.append({"title": f"{hash_}", "row": i + 1, "col": 1,
                     "data": {"df": df_total, "fields": [{"field": "value", "color": "blue"},
                                                         {"field": "v", "color": "yellow"}]}},
                    )
plot_dataframes(df_plots)


# get post_processed data from influx
influx_post = {}
for hash_ in post_harm.keys():
    influx_post[hash_] = beelib.beeinflux.get_timeseries_by_hash(hash_,
                                                                 "PT15M", config['influx'], ts_ini, ts_end)

# COMPARE POST WITH INFLUX
for hash_ in post_harm.keys():
    comp1 = post_harm[hash_][['value']]
    comp1['value'] = comp1.value.round(2)
    comp2 = influx_post[hash_][['value']]
    comp2.index = comp2.index.set_names("timestamp")
    comp2['value'] = comp2.value.round(2)
    comp1 = comp1.loc[comp2.index.min(): comp2.index.max()]
    comp2 = comp2.loc[comp1.index.min(): comp1.index.max()]
    try:
        c = comp1.compare(comp2)
        if not c.empty:
            raise Exception("different")
    except Exception as e:
        df_plots = [
            {"title": f"Harmonized:{hash_}", "row": 1, "col": 1,
             "data": [{"df": comp1, "fields": [{"field": "value", "color": "blue", "title": "here"}]},
                      {"df": comp2, "fields": [{"field": "value", "color": "red", "title": "influx"}]}
                      ]
             }
        ]
        plot_dataframes(df_plots)
        break



# CALCULATION DEVICES

calc_query = f"""
                MATCH (m)-[:s4city__quantifiesKPI|:saref__relatesToProperty]->(prop) 
                WHERE m.bee__calculationFormula IS NOT NULL AND m.bigg__measurementFrequency="{freq['freq']}"
                RETURN {{bigg__hash: m.bigg__hash, 
                bee__calculationFormula: m.bee__calculationFormula, 
                bigg__measurementFrequency: m.bigg__measurementFrequency,
                property:prop.uri}} AS m
            """
devices = [d['m'] for d in driver.session().run(calc_query).data()]
devices = [d for d in devices if d['bigg__hash'] in calculation_hashes]

all_results = {}
for dev in devices:
    formula = dev["bee__calculationFormula"]
    formula = f"<root>{formula}</root>"
    formula_tree = ElementTree.fromstring(formula)
    calculator = CalculateFunctions({}, {}, config['influx'], config['neo4j'],
                                    "influx")
    try:
        components = calculator.get_timeseries_components(formula_tree, ts_ini, ts_end, freq['freq'])
        result = calculator.calculate_formula(formula_tree, ts_ini, ts_end, freq['freq'])
    except Exception as e:
        print("error with calculations")
    all_results[dev['bigg__hash']] = result

df_plots = []
# PLOT CALCULATION
for dev in devices:
    reg = r"<(?P<t>m.)>(?P<v>[\w\+\-\*/]*)</(?P<t1>m.)>"
    dev['formula'] = re.findall(reg, dev["bee__calculationFormula"])
    for i, component in enumerate([x for x in dev['formula'] if x[0] in ['mh', 'mc', 'mv']]):
        df_plots.append({"title": f"comp: {component[1]}", "row": i + 1, "col": 1,
                         "data": {"df": post_harm[component[1]], "fields": ["value"]}})
    df_plots.append({"title": f"calc: {dev['bigg__hash']}", "row": i + 2, "col": 1,
                     "data": {"df": all_results[dev['bigg__hash']], "fields": ["value"]}})
plot_dataframes(df_plots)

# get calculation data from influx
influx_calc = {}
for hash_ in all_results:
    influx_calc[hash_] = beelib.beeinflux.get_timeseries_by_hash(hash_,
                                                                 "PT15M", config['influx'], ts_ini, ts_end)
# COMPARE POST WITH INFLUX
for hash_ in all_results.keys():
    comp1 = all_results[hash_][['value']]
    comp1['value'] = comp1.value.round(2)
    comp2 = influx_calc[hash_][['value']]
    comp2.index = comp2.index.set_names("timestamp")
    comp2['value'] = comp2.value.round(2)
    try:
        c = comp1.compare(comp2)
        if not c.empty:
            raise Exception("different")
    except Exception as e:
        df_plots = [
            {"title": f"Harmonized:{hash_}", "row": 1, "col": 1,
             "data": [{"df": comp1, "fields": [{"field": "value", "color": "blue", "title": "here"}]},
                      {"df": comp2, "fields": [{"field": "value", "color": "red", "title": "influx"}]}
                      ]
             }
        ]
        plot_dataframes(df_plots)

