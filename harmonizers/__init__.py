import pickle
import time
from itertools import count

import redis
import beelib
import neo4j
import pandas as pd
import xml.etree.ElementTree as ElementTree
from harmonizers.post_process.pv_postprocess import PVProcessor
from harmonizers.sources.dexma import dexma, dexma_projects
from harmonizers.sources.modbus import modbus
from harmonizers.sources.ixon import ixon
from harmonizers.sources.meteogalicia import meteogalicia
from harmonizers.sources.agbar import agbar
from harmonizers.sources.manttest import manttest, manttest_eco
from harmonizers.sources.bacnet import bacnet
from lib2 import harmonize_irregular_data, send_to_kafka
from lib2.calculate_formulas import CalculateFunctions
import logging
from pythonjsonlogger import jsonlogger
import numpy as np


logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter('%(asctime)s - %(levelname)s - %(name)s: %(message)s')
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)

sources = [dexma, dexma_projects, meteogalicia, modbus, bacnet, agbar, ixon, manttest, manttest_eco]

post_processors = [PVProcessor]


def create_redis_name(queue, freq, diff):
    return ".".join([x for x in [queue, freq, diff] if x])


def init_redis(redis_conn, freq, diff, num_processors):
    redis_base = create_redis_name("harmonizer.start", freq['freq'], diff)
    redis_conn.set(f"{redis_base}.semaphore", 0)
    for source_config in sources:
        redis_base = create_redis_name(f"harmonizer.{source_config['name'].lower()}", freq['freq'], diff)
        redis_conn.set(f"{redis_base}.semaphore", num_processors)
        redis_conn.delete(f"{redis_base}.queue")
    for post in post_processors:
        redis_base = create_redis_name(f"harmonizer.{post.name.lower()}", freq['freq'], diff)
        redis_conn.set(f"{redis_base}.semaphore", num_processors)
        redis_conn.delete(f"{redis_base}.queue")

    redis_base = create_redis_name("harmonizer.calculation.0", freq['freq'], diff)
    redis_conn.set(f"{redis_base}.semaphore", num_processors)
    redis_conn.delete(f"{redis_base}.queue")
    redis_base = create_redis_name("harmonizer.calculation", freq['freq'], diff)
    redis_conn.set(f"{redis_base}.max_prio", 1)

    redis_base = create_redis_name("harmonizer.limits", freq['freq'], diff)
    redis_conn.set(f"{redis_base}.semaphore", num_processors)
    redis_conn.delete(f"{redis_base}.queue")


def wait_sync_redis(redis_conn, queue, freq, diff, num_proc, time_min):
    timeout = time.time() + 60 * time_min  # timeout  minutes
    redis_base = create_redis_name(queue, freq['freq'], diff)
    while True:
        try:
            ticket = int(redis_conn.get(f"{redis_base}.semaphore"))
        except Exception:
            ticket = 0
        if ticket:
            l = int(redis_conn.decr(f"{redis_base}.semaphore"))
            if l < 0:
                raise ValueError("The semaphore can't be negative")
            logger.debug("Ticket obtained", extra={'phase': "GATHER", "source": f"{redis_base}.semaphore",
                                                   "ticket": l})
            break
        time.sleep(1)
        if time.time() > timeout:
            raise TimeoutError("The process starting was too slow")
    while True:
        logger.debug("Waiting All tickets", extra={'phase': "GATHER", "source": f"{redis_base}.semaphore",
                                                   "ticket": l})
        try:
            ticket = int(redis_conn.get(f"{redis_base}.semaphore"))
        except Exception:
            ticket = 0
        if ticket == 0:
            logger.debug("All sync", extra={'phase': "GATHER", "source": f"{redis_base}.semaphore",
                                            "ticket": l})
            return
        if time.time() > timeout:
            raise TimeoutError("The process starting was too slow")


def starter_job(neo4j_connection, redis_connection, freq, diff, num_processors, actions):
    logger.info("Starting the ingestor", extra={'phase': "START"})
    driver = neo4j.GraphDatabase.driver(**neo4j_connection)
    redis_conn = redis.Redis(**redis_connection)
    # set all redis to initial state
    init_redis(redis_conn, freq, diff, num_processors)
    # SET devices by source
    if "raw" in actions:
        for source_config in sources:
            redis_base = create_redis_name(f"harmonizer.{source_config['name'].lower()}", freq['freq'], diff)
            logger.info("Reading from", extra={'phase': "GATHER", "source": source_config['name']})
            source_devices = pd.DataFrame()
            for s_t in source_config['device_query']:
                try:
                    devices = []
                    for attempt in range(5):
                        try:
                            devices = driver.session().run(s_t.format(freq=freq['freq'])).data()
                            break
                        except Exception as e:
                            pass
                    if not devices:
                        continue
                    device_df = pd.json_normalize(devices).explode("raw_data")
                    device_df = pd.concat([device_df.drop('raw_data', axis=1),
                                           device_df['raw_data'].apply(pd.Series).rename(
                                               columns={c: f"raw_data.{c}" for c in
                                                        device_df['raw_data'].apply(pd.Series).columns})], axis=1)
                    source_devices = pd.concat([source_devices, device_df])
                except KeyError:
                    pass
            if source_devices.empty:
                logger.info("No devices from source", extra={'phase': "GATHER", "source": source_config['name']})
                continue
            logger.info("Readed", extra={'phase': "GATHER", "source": source_config['name'],
                                         "devices": len(source_devices)})
            for _, dev in source_devices.iterrows():
                redis_conn.lpush(f"{redis_base}.queue",
                                 pickle.dumps(dev.to_dict()))
    # SET devices by post processors
    if "processors" in actions:
        for post in post_processors:
            logger.info("Applying post Processor", extra={'phase': "GATHER", "source": post.name})
            redis_base = create_redis_name(f"harmonizer.{post.name.lower()}", freq['freq'], diff)
            query = post.get_devices(freq)
            devices = driver.session().run(query).data()
            if not devices:
                continue
            logger.info("Applying over devices", extra={'phase': "GATHER", "source": post.name,
                                                        "devices": len(devices)})
            for dev in devices:
                redis_conn.lpush(f"{redis_base}.queue", pickle.dumps(dev))
    # SET devices for calculation formulas
    if "calculations" in actions:
        logger.info("Applying post Calculations", extra={'phase': "GATHER", "source": "Calculation"})
        calc_query = f"""
                MATCH (m)-[:s4city__quantifiesKPI|:saref__relatesToProperty]->(prop) 
                WHERE m.bee__calculationFormula IS NOT NULL AND m.bigg__measurementFrequency="{freq['freq']}"
                RETURN {{bigg__hash: m.bigg__hash, 
                bee__calculationFormula: m.bee__calculationFormula, 
                bigg__measurementFrequency: m.bigg__measurementFrequency,
                property:prop.uri}} AS m
            """
        devices = driver.session().run(calc_query).data()
        calculator = CalculateFunctions({}, {}, {}, neo4j_connection,
                                        {})
        devices_priority = calculator.order_by_dependencies(devices)
        prio = 0
        for prio, devices in devices_priority.items():
            redis_base = create_redis_name(f"harmonizer.calculation.{prio}", freq['freq'], diff)
            redis_conn.set(f"{redis_base}.semaphore", num_processors)
            logger.info("Applying Calculations over devices", extra={'phase': "GATHER", "source": "Calculation",
                                                                     "devices": len(devices), "priority": prio})
            redis_conn.delete(f"{redis_base}.queue")
            for dev in [x['m'] for x in devices]:
                redis_conn.lpush(f"{redis_base}.queue", pickle.dumps(dev))
        redis_base = create_redis_name(f"harmonizer.calculation", freq['freq'], diff)
        redis_conn.set(f"{redis_base}.max_prio", prio)

    if "limits" in actions:
        logger.info("Applying compliance", extra={'phase': "GATHER", "source": "Compliance"})
        limits_query = f"""
        match (property:s4city__KeyPerformanceIndicator)<-[:s4city__quantifiesKPI]-(kpi)-[:s4city__assesses]->(dev:bee__BmsDevice)-
        [:saref__makesMeasurement]->(lower:bigg__LimitMeasurement)-[s4ener__hasUsage]->(l:s4ener__Usage) where lower.uri contains
        'limit-lower' and kpi.bigg__measurementFrequency="{freq['freq']}" and property.uri contains 'Compliance'
        match (dev)-[:saref__makesMeasurement]->(upper:bigg__LimitMeasurement) where upper.uri contains 'limit-upper'
        match (dev)-[:saref__makesMeasurement]->(dev_m:saref__Measurement)
        match (dev)-[:bigg__isConditionedBy]->(dev_cond)
        match (dev_cond)-[:saref__makesMeasurement]->()<-[:owl__sameAs]-(cond_m)
        return distinct({{kpi__hash:kpi.bigg__hash, bigg__measurementFrequency:kpi.bigg__measurementFrequency,
        lower__activation:lower.bee__activationLimit, lower__formula:lower.bee__calculationFormula, upper__activation:upper.bee__activationLimit,
        upper__formula:upper.bee__calculationFormula,measurement_hash:dev_m.bigg__hash, property:property.uri}}) as m
        """
        devices = driver.session().run(limits_query).data()
        redis_base = create_redis_name(f"harmonizer.limits", freq['freq'], diff)

        if devices:
            logger.info("Applying over devices", extra={'phase': "GATHER", "source": "Compliance",
                                                        "devices": len(devices)})

            for dev in [x['m'] for x in devices]:
                redis_conn.lpush(f"{redis_base}.queue", pickle.dumps(dev))

    redis_base = create_redis_name("harmonizer.start", freq['freq'], diff)
    redis_conn.set(f"{redis_base}.semaphore", num_processors)
    logger.debug("Redis Base", extra={'phase': "GATHER", "source": f"{redis_base}.semaphore"})


def processor_job(redis_connection, kafka_connection, hbase_connection, druid_topic, druid_connection, druid_datasource,
                  influx_connection, neo4j_connection, ts_ini, ts_end, freq, diff, num_processors, actions):
    redis_conn = redis.Redis(**redis_connection)
    logger.debug("Wait To Start", extra={'phase': "GATHER"})
    wait_sync_redis(redis_conn, "harmonizer.start", freq, diff, num_processors, 5)
    # process raw_data
    druid_producer = beelib.beekafka.create_kafka_producer(kafka_connection, encoding="JSON")
    if "raw" in actions:
        for source_config in sources:
            logger.debug("Processing from", extra={'phase': "GATHER", "source": source_config['name'].lower()})
            redis_base = create_redis_name(f"harmonizer.{source_config['name'].lower()}", freq['freq'], diff)
            while True:
                dev = redis_conn.rpop(f"{redis_base}.queue")
                if not dev:
                    break
                dev = pickle.loads(dev)
                logger.debug("Processing from", extra={'phase': "GATHER", "source": source_config['name'].lower(),
                                                       "len": redis_conn.llen(f"{redis_base}.queue"),
                                                       "dev": dev})
                s = time.time()
                try:
                    df_device_final = harmonize_raw_data(dev, source_config, hbase_connection, ts_ini, ts_end, freq)
                    if (df_device_final is not None) and not df_device_final.empty:
                        df_device_final['property'] = (dev['harmonized.property'].
                                                       replace("https://bigg-project.eu/ontology#", "").
                                                       replace("https://saref.etsi.org/core/", "").
                                                       replace("https://www.beegroup-cimne.com/bee/ontology#", ""))
                        df_device_final['hash'] = dev['harmonized.bigg__hash']
                        df_device_final['value'] = df_device_final['value'].round(5)
                        df_to_save = df_device_final.reset_index().apply(
                            beelib.beedruid.harmonize_for_druid, timestamp_key="timestamp", value_key="value",
                            hash_key="hash",
                            property_key="property", is_real=True, freq=freq['freq'],
                            axis=1)
                        if df_to_save.empty:
                            continue
                        logger.debug("time harmonize + hbase",
                                     extra={'phase': "GATHER", "source": source_config['name'], "time": time.time() - s})
                        s = time.time()
                        data = send_to_kafka(druid_producer, druid_topic, df_to_save)
                        logger.debug("time kafka",
                                     extra={'phase': "GATHER", "source": source_config['name'], "time": time.time() - s})
                except Exception as e:
                    logger.error("Failed to harmonize raw",
                                 extra={'phase': "GATHER", "dev": dev, "source": source_config['name'], "error": str(e)})

            wait_sync_redis(redis_conn, f"harmonizer.{source_config['name'].lower()}",
                            freq, diff, num_processors, 60)
    # post processors
    if "processors" in actions:
        for post in post_processors:
            logger.debug("Processing from", extra={'phase': "GATHER", "source": post.name.lower()})
            redis_base = create_redis_name(f"harmonizer.{post.name.lower()}", freq['freq'], diff)
            while True:
                dev = redis_conn.rpop(f"{redis_base}.queue")
                if not dev:
                    break
                dev = pickle.loads(dev)
                logger.debug("Processing from", extra={'phase': "GATHER", "source": post.name.lower(),
                                                       "len": redis_conn.llen(f"{redis_base}.queue"),
                                                       "dev": dev})
                try:
                    post.process_device(dev, druid_producer, druid_connection, druid_datasource, druid_topic,
                                        influx_connection, ts_ini, ts_end, freq)
                except Exception as e:
                    logger.error("Failed to postProcess devices",
                                 extra={'phase': "GATHER", "dev": dev, "source": post.name.lower(), "error": str(e)})

            wait_sync_redis(redis_conn, f"harmonizer.{post.name.lower()}",
                            freq, diff, num_processors, 60)

    # calculation data
    if "calculations" in actions:
        try:
            redis_base = create_redis_name(f"harmonizer.calculation", freq['freq'], diff)
            max_prio = int(redis_conn.get(f"{redis_base}.max_prio")) + 1
        except:
            max_prio = 1

        for prio in range(0, max_prio):
            logger.debug("Processing from", extra={'phase': "GATHER", "source": "calculation", "prio": prio})
            redis_base = create_redis_name(f"harmonizer.calculation.{prio}", freq['freq'], diff)
            while True:
                dev = redis_conn.rpop(f"{redis_base}.queue")
                if not dev:
                    break
                dev = pickle.loads(dev)
                logger.debug("Processing from", extra={'phase': "GATHER", "source": "calculation",
                                                       "len": redis_conn.llen(f"{redis_base}.queue"),
                                                       "dev": dev})
                try:
                    harmonize_calculation_devices(dev, druid_connection, druid_datasource, druid_producer, druid_topic,
                                                  influx_connection, ts_ini, ts_end, freq, neo4j_connection)
                except Exception as e:
                    logger.error("Failed to calculate devices",
                                 extra={'phase': "GATHER", "dev": dev, "source": "calculation", "error": str(e)})
            wait_sync_redis(redis_conn, f"harmonizer.calculation.{prio}",
                            freq, diff, num_processors, 60)

    if "limits" in actions:
        logger.debug("Processing from", extra={'phase': "GATHER", "source": "Compliance"})
        redis_base = create_redis_name(f"harmonizer.limits", freq['freq'], diff)
        while True:
            dev = redis_conn.rpop(f"{redis_base}.queue")
            if not dev:
                break
            dev = pickle.loads(dev)
            logger.debug("Processing from", extra={'phase': "GATHER", "source":"Compliance",
                                                   "len": redis_conn.llen(f"{redis_base}.queue"),
                                                   "dev": dev['kpi__hash']})
            try:
                harmonize_limits(dev, druid_connection, druid_datasource, druid_producer, druid_topic,
                                              influx_connection, ts_ini, ts_end, freq, neo4j_connection)
                logger.debug("Limit processed", extra={'phase': "GATHER", "source": "Compliance",
                                                       "len": redis_conn.llen(f"{redis_base}.queue"),
                                                       "dev": dev['kpi__hash']})
            except Exception as e:
                logger.error(f"Failed to compliance devices {e}",
                             extra={'phase': "GATHER", "dev": dev['kpi__hash'], "source": "Compliance", "error": str(e)})

        wait_sync_redis(redis_conn, "harmonizer.limits",
                        freq, diff, num_processors, 60)
    redis_conn.delete(f"{create_redis_name('harmonizer.start', freq['freq'], diff)}.semaphore")


def get_raw_data(dev, source_config, hbase_connection, ts_ini, ts_end, freq):
    reg_freq = dev['raw_data.freq']
    s = time.time()
    raw_data = source_config['raw_data'](**eval(source_config['raw_data_args']))
    logger.debug("time hbase", extra={'phase': "GATHER", "source": source_config['name'], "time": time.time() - s})
    if raw_data.empty:
        return pd.DataFrame()
    raw_data['timestamp'] = pd.to_datetime(raw_data['ts'].astype(int), unit="s")
    raw_data['raw_conv_ratio'] = dev['raw_data.raw_unitConversionRatio']
    raw_data['raw_conv_offset'] = dev['raw_data.raw_unitConversionOffset']
    raw_data['harm_conv_ratio'] = dev['harmonized.harmonized_unitConversionRatio']
    raw_data['harm_conv_offset'] = dev['harmonized.harmonized_unitConversionOffset']
    raw_data['property'] = (dev['harmonized.property'].
                            replace("https://bigg-project.eu/ontology#", "").
                            replace("https://saref.etsi.org/core/", "").
                            replace("https://www.beegroup-cimne.com/bee/ontology#", ""))
    raw_data['value'] = raw_data.apply(source_config['value'], axis=1, args=(dev,))
    raw_data = raw_data.set_index('timestamp').sort_index().dropna(subset=['value'])
    if raw_data.empty:
        return pd.DataFrame()
    raw_data = raw_data[~raw_data.index.duplicated(keep='last')]
    return raw_data


def harmonize_raw_data(dev, source_config, hbase_connection, ts_ini, ts_end, freq):
    raw_data = get_raw_data(dev, source_config, hbase_connection, ts_ini, ts_end, freq)
    if raw_data.empty:
        return
    table_freq = raw_data['freq'].unique()[0]
    # check for big gaps to harmonize separately
    start_index = raw_data.index.min() - pd.DateOffset(seconds=1)
    parts = []
    if freq['gap_check']:
        check = raw_data.index.diff(1).fillna(pd.Timedelta(0)).total_seconds()
        big_gap_index = raw_data.loc[check > freq['gap_check']].index
        for end_idx in big_gap_index:
            part = raw_data.loc[start_index:end_idx - pd.DateOffset(seconds=1)]
            parts.append(part)
            start_index = end_idx
    parts.append(raw_data.loc[start_index:])
    # deal with each part individually as they are continuous
    df_device_final = pd.DataFrame()
    if not table_freq:
        for df_h in parts:
            df_h = source_config['pre_clean'](df_h, dev)
            df_h = df_h.dropna(subset=['value'])
            if df_h.empty:
                continue
            agg_func = dev['harmonized.aggregationFunction']
            df_tmp = harmonize_irregular_data(df_h, agg_func, freq['freq'], "value")
            df_device_final = pd.concat([df_device_final, df_tmp])
    else:
        for df_h in parts:
            df_device_final = pd.concat([df_device_final, df_h])
    if df_device_final.empty:
        return
    df_device_final = source_config['post_clean'](df_device_final, dev)
    df_device_final = df_device_final.loc[
                      df_device_final.index.min() + pd.DateOffset(days=freq['days_to_overlap']):]
    return df_device_final


def harmonize_calculation_devices(dev, druid_connection, druid_datasource, druid_producer, druid_topic,
                                  influx_connection, ts_ini, ts_end, freq, neo4j_connection):
    """
    calculates the formula with the following schema:
    mo: operation (+ - * /)
    mbr: parentheses ()
    mh: get data from hash
    mc: get data from constant defined at KPIS_FACTORS
    mv: get data from value
    mq: get data from neo4j query
    fun: predefined function
        fop: the type of the function
        fpar: the parameter of the function
    :param dev:
    :param druid_connection:
    :param druid_datasource:
    :param druid_producer:
    :param druid_topic:
    :param influx_connection:
    :param ts_ini:
    :param ts_end:
    :param freq:
    :param neo4j_connection:
    :return:
    while True:
        dev = redis_conn.rpop(f"{redis_base}.queue")
        if not dev:
            print("finish")
            break
        dev = pickle.loads(dev)
        formula = dev["bee__calculationFormula"]
        formula = f"<root>{formula}</root>"
        formula_tree = ElementTree.fromstring(formula)
        calculator = CalculateFunctions(druid_connection, druid_datasource, influx_connection, neo4j_connection,
                                       druid_topic)
        try:
            result = calculator.calculate_formula(formula_tree, ts_ini, ts_end, freq['freq'])
            result.dropna(subset=['value'], inplace=True)
            if not result.empty:
                print(found)
                break
        except Exception as e:
            print(e)
    """
    formula = dev["bee__calculationFormula"]
    formula = f"<root>{formula}</root>"
    formula_tree = ElementTree.fromstring(formula)
    calculator = CalculateFunctions(druid_connection, druid_datasource, influx_connection, neo4j_connection,
                                    druid_topic)
    try:
        result = calculator.calculate_formula(formula_tree, ts_ini, ts_end, freq['freq'])
    except Exception as e:
        return None
    result.dropna(subset=["value"], inplace=True)
    if result.empty:
        return None
    result['hash'] = dev['bigg__hash']
    result['property'] = (dev['property'].
                          replace("https://bigg-project.eu/ontology#", "").
                          replace("https://saref.etsi.org/core/", "").
                          replace("https://www.beegroup-cimne.com/bee/ontology#", ""))
    result.reset_index(names="start", inplace=True)
    df_to_save = result.reset_index().apply(
        beelib.beedruid.harmonize_for_druid, timestamp_key="start", value_key="value", hash_key="hash",
        property_key="property", is_real=True,
        freq=dev['bigg__measurementFrequency'],
        axis=1)
    data = send_to_kafka(druid_producer, druid_topic, df_to_save)
    return data


def harmonize_limits(dev, druid_connection, druid_datasource, druid_producer, druid_topic,
                     influx_connection, ts_ini, ts_end, freq, neo4j_connection):

    lower_activation = ElementTree.fromstring(f"<root>{dev['lower__activation']}</root>")
    lower_formula = ElementTree.fromstring(f"<root>{dev['lower__formula']}</root>")
    upper_activation = ElementTree.fromstring(f"<root>{dev['upper__activation']}</root>")
    upper_formula = ElementTree.fromstring(f"<root>{dev['upper__formula']}</root>")

    calculator = CalculateFunctions(druid_connection, druid_datasource, influx_connection, neo4j_connection,
                                    druid_topic)
    try:
        lower_activation_ts = calculator.calculate_formula(lower_activation, ts_ini, ts_end, freq['freq'])
        lower_formula_ts = calculator.calculate_formula(lower_formula, ts_ini, ts_end, freq['freq'])
        upper_activation_ts = calculator.calculate_formula(upper_activation, ts_ini, ts_end, freq['freq'])
        upper_formula_ts = calculator.calculate_formula(upper_formula, ts_ini, ts_end, freq['freq'])

        dev_measurement = beelib.beeinflux.get_timeseries_by_hash(dev['measurement_hash'], freq['freq'], influx_connection,
                                                                  ts_ini, ts_end)

        lower_activation_ts = lower_activation_ts.replace(0, np.nan).dropna()
        lower_activation_ts.value = 1.0
        upper_activation_ts = upper_activation_ts.replace(0, np.nan).dropna()
        upper_activation_ts.value = 1.0

        df = pd.DataFrame({
            'upper_activation': upper_activation_ts['value'],
            'lower_activation': lower_activation_ts['value'],
            'lower_limit': lower_formula_ts['value'],
            'upper_limit': upper_formula_ts['value'],
            'measurement': dev_measurement['value']
        })

        df = df.dropna(subset=['upper_activation', 'lower_activation', 'lower_limit', 'upper_limit', 'measurement'])
        df['value'] = df['measurement'].between(df['lower_limit'], df['upper_limit'])
        df.value = df.value.astype(int)

    except Exception as e:
        return None
    df.index.name = 'start'
    df['hash'] = dev['kpi__hash']
    df['property'] = "Compliance"
    df.reset_index(names="start", inplace=True)
    df.dropna(subset=["value"], inplace=True)
    df_to_save = df.reset_index().apply(
        beelib.beedruid.harmonize_for_druid, timestamp_key="start", value_key="value", hash_key="hash",
        property_key="property", is_real=True,
        freq=dev['bigg__measurementFrequency'],
        axis=1)
    data = send_to_kafka(druid_producer, druid_topic, df_to_save)
    return data
