import os
import dotenv
import neo4j
import numpy as np
import time
import pandas as pd
import lib
import sys
import datetime
import logging
import beelib
import argparse

max_32_value = (2**31)

TABLE_RAW = "ixon:raw_ixon_ts_time_{property}_"
TABLE_HARMONIZED = "icat:harmonized_online_{data_type}_100_{agg}_{freq}_icat"
DEXMA_RAW_TABLE = "dexma:raw_dexma_ts_time_{property}_PT15M_"
DEXMA_PARAMETERS = [
    'EACTIVE',
    'PAENERGY'
    ]

DEXMA_DEVICES_QUERY = """Match (dx:bigg__DexmaDevice)-[:saref__measuresProperty]->(p) 
                Match (dx)-[:bigg__measuresIn]->(devu) Match (dx)-[:saref__hasMeasurement]->(m)
                -[:saref__isMeasuredIn]->(propu) Match (ds:bigg__DexmaSystem)-[:s4syst__hasSubSystem]->(dx) 
                return p.bigg__aggregationFunction as agg_func, p.uri as table, devu.qudt__conversionMultiplier 
                as dev_conv, propu.qudt__conversionMultiplier as prop_conv, m.bigg__hash as hash, 
                dx.bigg__deviceId as id, dx.bigg__parameter as parameter, ds.bigg__locationDexma as name"""

MODBUS_DEVICES_QUERY = """Match (d:bigg__PvModbusDevice)-[:saref__hasMeasurement]->(m:saref__Measurement) return d.uri as uri, m.bigg__hash as hash
                        """

PHOTOVOLTAIC_QUERY = """MATCH (n:bigg__PhotoVoltaicSystem)-[:saref__hasMeasurement]->
                    (m)-[:saref__relatesToProperty]-(p)return m.bigg__hash as hash,
                    n.bigg__calcCase as calcCase, n.bigg__exportToGrid as export,
                    n.bigg__idFromManttest as id, split(p.uri,"#")[1] as property"""

KPIS_QUERY = """
                SELECT  TIMESTAMP_TO_MILLIS("__time")/1000 AS "timestamp", LATEST("end") AS "end", "hash" as "measurement", "isReal", LATEST("value") as "value", "property"
                FROM "icat.druid"
                WHERE "property" = '%s' AND "hash" IN %s AND TIMESTAMP_TO_MILLIS("__time")/1000 >= %s AND TIMESTAMP_TO_MILLIS("__time")/1000 <= %s
                GROUP BY "__time", "hash", "isReal", "property"
           
            """

DRUID_TOPIC = 'icat.druid'

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(sys.stdout))


def get_table(row):
    table = row['table']
    table_name = table.split('#')[1]
    return table_name


def znorm_clean(df):
    if len(df) < 108:
        window = 24
    else:
        window = 96
    #window = min(, len(df)-1)
    for i in range(window, len(df), 1):
        df_t = df.iloc[max(i - window, 0): i - 1]
        mean = df_t.value_calc.mean()
        std = df_t.value_calc.std()
        zscore = abs(df.value_calc[i] - mean) / std
        if zscore > 30:
            df['value_calc'][i] = np.nan
    return df[window:]


def get_neo4j_data(query, conf):
    count = 0
    while count < 10:
        try:
            driver = neo4j.GraphDatabase.driver(**conf['neo4j'])
            with driver.session() as session:
                dict_data = session.run(query).data()
            data_df = pd.DataFrame(dict_data)
            return data_df
        except Exception as e:
            print(e)
            print(f"attempt {count}")
            count += 1
            time.sleep(1)
    if count >= 10:
        return None


def harmonize_dexma(row_start, row_stop, druid_producer, conf, save=True):
    logger.debug("###### DEXMA ######")
    dev_df = get_neo4j_data(DEXMA_DEVICES_QUERY, conf)
    dev_df['property'] = dev_df.apply(get_table, axis=1)
    df_info = []
    dev_ids = []
    init = time.time()
    for parameter in DEXMA_PARAMETERS:
        curr_table_raw = DEXMA_RAW_TABLE.format(property=parameter)
        print(curr_table_raw)
        devices = []
        for data in beelib.beehbase.get_hbase_data_batch(conf['hbase']['connection'], curr_table_raw, row_start=row_start, row_stop=row_stop):
            for key, row in data:
                try:
                    ts, dev_info = key.decode("utf-8").split("~")
                    dev = dev_df[dev_df['id'] == dev_info]
                    if dev.empty:
                        continue
                    prop = dev['property'].unique()
                    if len(prop) > 1:
                        continue
                    else:
                        prop = prop[0]
                    for hash_ in dev_df[dev_df['id'] == dev_info]['hash'].unique():
                        item = {'device': dev_info, 'ts': int(ts), 'parameter': parameter, 'property': prop,
                                'hash': hash_}
                        for k, v in row.items():
                            item[k.decode("utf-8").split(":")[1]] = v.decode("utf")
                        df_info.append(item)
                        devices.append(dev_info)
                except Exception as e:
                    dev_ids.append(dev_info)
        print(time.time() - init)

    df = pd.DataFrame.from_records(df_info)
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['ts'].astype(int), unit="s")
        if save:
            df_to_save = df.reset_index().apply(lib.create_harmonized_druid, timestamp_key="timestamp",
                                                value_key="v", axis=1)
            for data in df_to_save.to_list():
                druid_producer.send(DRUID_TOPIC, data)
        else:
            return df


def harmonize_modbus(row_start, row_stop, druid_producer, conf, save=True):
    logger.debug("###### MODBUS ######")
    dev_df = get_neo4j_data(MODBUS_DEVICES_QUERY, conf)
    df_data_timeseries = pd.DataFrame()
    for prop, prop_config in lib.property_configs.items():
        curr_table_raw = TABLE_RAW.format(property=prop)
        x = time.time()
        df_info = []
        for data in beelib.beehbase.get_hbase_data_batch(conf['hbase']['connection'], curr_table_raw, row_start=row_start, row_stop=row_stop):
            for key, row in data:
                ts, dev_info = key.decode("utf-8").split("~")
                item = {'device': dev_info, 'ts': int(ts), "property": prop}
                for k, v in row.items():
                    item[k.decode("utf-8").split(":")[1]] = v.decode("utf")
                df_info.append(item)
        print(time.time()-x)
        df_data_timeseries = pd.concat([df_data_timeseries, pd.DataFrame(df_info)], ignore_index=True)
    removed = df_data_timeseries[df_data_timeseries['value'].astype(float) == float(max_32_value)]
    df_data_timeseries = df_data_timeseries[df_data_timeseries['value'].astype(float) != float(max_32_value)]
    df_data_timeseries = lib.create_calc_value_from_records(df_data_timeseries)
    df_final = pd.DataFrame()
    for device, df_device in df_data_timeseries.groupby("device"):
        prop = df_device.property.unique()
        if len(prop) > 1:
            print(f"non unique device: {device}")
            continue
        prop = prop[0]
        prop_config = lib.property_configs[prop]
        df_device_final = pd.DataFrame()
        df_device['timestamp'] = pd.to_datetime(df_device['ts'].astype(int), unit="s")
        df_device.set_index("timestamp", inplace=True)
        df_device.sort_index(inplace=True)
        df_device = df_device[~df_device.index.duplicated(keep='last')]
        df_device = znorm_clean(df_device)
        df_device = df_device.dropna(subset=['value_calc'])
        check = df_device.index.diff(1).fillna(pd.Timedelta(0)).total_seconds()
        big_gap_index = df_device.loc[check > 3600*2].index # calcule forats de més d'un hora
        parts = []
        start_index = df_device.index.min() - pd.DateOffset(seconds=1)
        for end_idx in big_gap_index:
            part = df_device.loc[start_index:end_idx - pd.DateOffset(seconds=1)]
            parts.append(part)
            start_index = end_idx
        parts.append(df_device.loc[start_index:]) #cree una llista amb totes les parts correctes
        for df in parts:
            if prop_config['agg_func'] == "SUM":
                df_resampled_total, df_resampled_count, df_resampled_sum = lib.harmonize_accumulated_values(df)
                df_tmp = pd.DataFrame(df_resampled_sum[df_resampled_count == 900])
                df_tmp['device'] = device
                df_device_final = pd.concat([df_device_final, df_tmp])
            elif prop_config['agg_func'] == "AVG":
                df_resampled_total, df_resampled_count, df_resampled_avg = lib.harmonize_instantaneous_values(df)
                df_tmp = pd.DataFrame(df_resampled_avg[df_resampled_count == 900])
                df_tmp['device'] = device
                df_device_final = pd.concat([df_device_final, df_tmp])
            elif prop_config['agg_func'] == "LAST":
                df_resampled_total, df_resampled_last = lib.harmonize_instantaneous_status(df)
                df_tmp = pd.DataFrame(df_resampled_last)
                df_tmp['device'] = device
                df_device_final = pd.concat([df_device_final, df_tmp])
        df_device_final['property'] = prop
        df_device_final['hash'] = df_device_final.device.map({x['uri']: x['hash'] for _, x in dev_df.iterrows()})
        df_final = pd.concat([df_final, df_device_final])
    if save:
        df_to_save = df_final.reset_index().apply(lib.create_harmonized_druid, timestamp_key="timestamp",
                                                  value_key="value_calc", axis=1)
        for data in df_to_save.to_list():
            druid_producer.send(DRUID_TOPIC, data)
    else:
        return df_final


def compute_kpis(row_start, row_stop, druid_producer, conf):
    logger.debug("######## KPIS ########")
    init = time.time()

    plant_df = get_neo4j_data(PHOTOVOLTAIC_QUERY, conf)

    prod_hash = tuple(plant_df[plant_df['property'] == 'EnergyGenerationElectricity']['hash'].unique())
    exp_hash = tuple(plant_df[plant_df['property'] == 'EnergyGenerationElectricityExportedToGrid']['hash'].unique())
    imp_hash = tuple(plant_df[plant_df['property'] == 'EnergyConsumptionElectricityImportedFromGrid']['hash'].unique())
    production = pd.DataFrame.from_records(beelib.beedruid.run_druid_query(conf['druid']['connection'], KPIS_QUERY % ('EnergyGenerationElectricity', prod_hash, row_start, row_stop)))
    exported = pd.DataFrame.from_records(beelib.beedruid.run_druid_query(conf['druid']['connection'], KPIS_QUERY % ('EnergyGenerationElectricityExportedToGrid', exp_hash, row_start, row_stop)))
    imported = pd.DataFrame.from_records(beelib.beedruid.run_druid_query(conf['druid']['connection'], KPIS_QUERY % ('EnergyConsumptionElectricityImportedFromGrid', imp_hash, row_start, row_stop)))

    if not production.empty:
        try:
            production['building'] = production.measurement.map({x['hash']: x['id'] for _, x in plant_df.iterrows()})
            exported['building'] = exported.measurement.map({x['hash']: x['id'] for _, x in plant_df.iterrows()})
            imported['building'] = imported.measurement.map({x['hash']: x['id'] for _, x in plant_df.iterrows()})
        except Exception as e:
            logger.debug(e)
            exit()

        for building, prod in production.groupby('building'):
            case = plant_df[plant_df['id'] == building]['calcCase'].unique()[0]
            if case == "1":
                pass#TODO
            # total = total[total['building'] == building]
            #
            # if not prod.empty and not total.empty:
            #     # calc auto
            #     calc_prop = 'EnergyConsumptionElectricitySelfConsumption'
            #     hash = plant_df[plant_df['id'] == building][plant_df['property'] == calc_prop]['hash'].unique()[0]
            #     auto = pd.DataFrame()
            #     auto['value'] = prod.set_index('timestamp')['value']
            #     auto['total'] = total.set_index('timestamp')['value']
            #     auto = auto.dropna()
            #     auto.loc[auto['value'] > auto['total'], 'value'] = auto['total']
            #     auto = auto['value'].reset_index(name='value')
            #     auto['timestamp'] = auto['timestamp'].apply(lambda x: datetime.datetime.fromtimestamp(x))
            #     df_to_save = auto.reset_index().apply(lib.create_harmonized_kpis, timestamp_key="timestamp",
            #                                           value_key="value", measurement=hash, axis=1)
            #     curr_table_harmonized = table_harmonized.format(data_type=calc_prop, freq="PT15M", agg="SUM")
            #     beelib.beehbase.save_to_hbase(df_to_save.to_list(), curr_table_harmonized, conf['hbase']['connection'],
            #                               [("v", ["value"]), ("info", ["end", "isReal"])],
            #                               ["bucket", "hash", "start"])
            #
            #     #calc exported
            #     calc_prop = "EnergyGenerationElectricityExportedToGrid"
            #     exported = prod.set_index('timestamp')['value'].sub(total.set_index('timestamp')['value']).reset_index(name='value')
            #     exported = exported.dropna()
            #     exported.loc[exported['value'] < 0, 'value'] = 0
            #     exported['timestamp'] = exported['timestamp'].apply(lambda x: datetime.datetime.fromtimestamp(x))
            #     curr_table_harmonized = table_harmonized.format(data_type=calc_prop, freq="PT15M", agg="SUM")
            #
            #
            #     #calc imported consum-generada
            #     calc_prop = "EnergyConsumptionElectricityImportedFromGrid"
            #     imported = total.set_index('timestamp')['value'].sub(prod.set_index('timestamp')['value']).reset_index(name='value')
            #     imported = imported.dropna()
            #     imported.loc[imported['value'] < 0, 'value'] = 0
            #     imported['timestamp'] = imported['timestamp'].apply(lambda x: datetime.datetime.fromtimestamp(x))
            #     curr_table_harmonized = table_harmonized.format(data_type=calc_prop, freq="PT15M", agg="SUM")

            elif case == "2":

                exp = exported[exported['building'] == building]
                imp = imported[imported['building'] == building]
                exp_to_grid = plant_df[plant_df['id'] == building]['export'].unique()[0]

                if not exp_to_grid:
                    exp = prod.copy()
                    exp['value'] = 0
                logger.debug(f"building: {building}, exported to grid: {exp_to_grid}")

                logger.debug(f"building: {building}, prod:{not prod.empty}, exported:{not exp.empty}, imported:{not imp.empty}")

                if not prod.empty and not exp.empty:
                    calc_prop = "EnergyConsumptionElectricitySelfConsumption"
                    auto = prod.set_index('timestamp')['value'].sub(exp.set_index('timestamp')['value']).reset_index(name='value')
                    auto = auto.dropna()
                    auto.loc[auto['value'] < 0, 'value'] = 0
                    auto['timestamp'] = auto['timestamp'].apply(lambda x: datetime.datetime.utcfromtimestamp(x))
                    auto['property'] = calc_prop
                    auto['hash'] = plant_df[plant_df['id'] == building][plant_df['property'] == calc_prop]['hash'].unique()[0]
                    df_to_save = auto.reset_index().apply(lib.create_harmonized_druid, timestamp_key="timestamp",
                                                          value_key="value", axis=1)
                    if not df_to_save.empty:

                        logger.debug(f"idFromMantTest {building}: EnergyConsumptionElectricitySelfConsumption")
                        for data in df_to_save.to_list():
                            druid_producer.send(DRUID_TOPIC, data)

                if not prod.empty and not exp.empty and not imp.empty:
                    calc_prop = "EnergyConsumptionElectricity"
                    total = prod.set_index('timestamp')['value'].add(imp.set_index('timestamp')['value']).sub(exp.set_index('timestamp')['value']).reset_index(name='value')
                    total = total.dropna()
                    total.loc[total['value'] < 0, 'value'] = 0
                    total['timestamp'] = total['timestamp'].apply(lambda x: datetime.datetime.utcfromtimestamp(x))
                    total['property'] = calc_prop
                    total['hash'] = plant_df[plant_df['id'] == building][plant_df['property'] == calc_prop]['hash'].unique()[0]
                    df_to_save = total.reset_index().apply(lib.create_harmonized_druid, timestamp_key="timestamp",
                                                           value_key="value", axis=1)
                    logger.debug(f"idFromManttest {building}: EnergyConsumptionElectricity")

                    if not df_to_save.empty:
                        for data in df_to_save.to_list():
                            druid_producer.send(DRUID_TOPIC, data)

            if not prod.empty:
                calc_prop = "CO2Emissions"
                co2 = (prod.set_index('timestamp')['value'] * lib.CO2_FACTOR).reset_index(name='value')
                co2.loc[co2['value'] < 0, 'value'] = 0
                co2['timestamp'] = co2['timestamp'].apply(lambda x: datetime.datetime.utcfromtimestamp(x))
                co2['property'] = calc_prop
                co2['hash'] = plant_df[plant_df['id'] == building][plant_df['property'] == calc_prop]['hash'].unique()[0]
                df_to_save = co2.reset_index().apply(lib.create_harmonized_druid, timestamp_key="timestamp",
                                                     value_key="value", axis=1)
                logger.debug(f"idFromMantTest {building}: CO2Emissions")
                for data in df_to_save.to_list():
                    druid_producer.send(DRUID_TOPIC, data)

                if not co2.empty:
                    calc_prop = "TreesEquivalent"
                    trees = (co2.set_index('timestamp')['value'] / lib.TREES_FACTOR).reset_index(name='value')
                    trees.loc[trees['value'] < 0, 'value'] = 0
                    trees['property'] = calc_prop
                    trees['hash'] = plant_df[plant_df['id'] == building][plant_df['property'] == calc_prop]['hash'].unique()[0]
                    df_to_save = trees.reset_index().apply(lib.create_harmonized_druid, timestamp_key="timestamp",
                                                           value_key="value", axis=1)
                    logger.debug(f"idFromMantTest {building}: TreesEquivalent")
                    if not df_to_save.empty:
                        for data in df_to_save.to_list():
                            druid_producer.send(DRUID_TOPIC, data)

                calc_prop = "HouseholdsEquivalent"
                house = (prod.set_index('timestamp')['value'] / lib.HOUSE_FACTOR).reset_index(name='value')
                house.loc[house['value'] < 0, 'value'] = 0
                house['timestamp'] = house['timestamp'].apply(lambda x: datetime.datetime.utcfromtimestamp(x))
                house['property'] = calc_prop
                house['hash'] = plant_df[plant_df['id'] == building][plant_df['property'] == calc_prop]['hash'].unique()[0]
                df_to_save = house.reset_index().apply(lib.create_harmonized_druid, timestamp_key="timestamp",
                                                       value_key="value", axis=1)
                if not df_to_save.empty:
                    for data in df_to_save.to_list():
                        druid_producer.send(DRUID_TOPIC, data)
    else:
        logger.debug("No production data for that time range")
    print(time.time() - init)


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--type', '-t', required=True, choices=["devices", "kpis"])
    ap.add_argument('--start', '-s', required=False, default=None)
    ap.add_argument('--stop', '-p', required=False, default=None)
    if os.getenv("PYCHARM_HOSTED") is not None:
        args = ap.parse_args(["-t", "devices", "-s", "2023-01-01", "-p", None])
    else:
        args = ap.parse_args()
        dotenv.load_dotenv()
        conf = beelib.beeconfig.read_config()
        druid_producer = beelib.beekafka.create_kafka_producer(conf['kafka'], encoding="JSON")
        if args.start is not None:
            row_start = str(int(datetime.datetime.fromisoformat(args.start).timestamp()))
        else:
            row_start = str(int((datetime.datetime.utcnow() - datetime.timedelta(days=2)).timestamp()))
        if args.stop is not None:
            row_stop = str(int(datetime.datetime.fromisoformat(args.stop).timestamp()))
        else:
            row_stop = str(int(datetime.datetime.utcnow().timestamp()))

        if args.type == 'devices':
            harmonize_dexma(row_start, row_stop, druid_producer, conf)
            harmonize_modbus(row_start, row_stop, druid_producer, conf)
        elif args.type == 'kpis':
            compute_kpis(row_start, row_stop, druid_producer, conf)
        druid_producer.flush()
        druid_producer.close()
