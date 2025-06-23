import base64
import datetime
import isodate
import neo4j
import numpy as np
import pandas as pd
import beelib
import xml.etree.ElementTree as ElementTree


def isodate_floor(timestamp, freq):
    if freq == "P1M":
        return timestamp.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif freq == "P1W":
        return timestamp.replace(hour=0, minute=0, second=0, microsecond=0) - pd.DateOffset(days=timestamp.weekday())
    else:
        return timestamp.floor(pd.Timedelta(freq))


def comparable_freq(freq):
    if freq == "P1M":
        return pd.Timedelta(days=30)
    else:
        return pd.Timedelta(freq)


def transform_freq(freq):
    if freq == "P1M":
        return f"1MS"
    else:
        return pd.Timedelta(freq)


class CalculateFunctions(object):
    def __init__(self, druid_connection, druid_datasource, influx_connection, neo4j_connection, source):
        self.druid_connection = druid_connection
        self.druid_datasource = druid_datasource
        self.influx_connection = influx_connection
        self.neo4j_connection = neo4j_connection
        self.KPIS_FACTORS = {'HOUSE_FACTOR': 8.76,
                             'CO2_FACTOR': 0.00009,
                             'TREES_FACTOR': 0.0022}
        self.OPERATION_MAP = {
            "SUM": "sum",
            "AVG": "mean",
            "LAST": "last"
        }
        self.source = source

        self.AVAILABLE_FUNCTIONS = {
            "CLIP": self.__clip__,
            "ABS": self.__absolute__,
            "HE": self.__he__
        }

    def _get_device_measurements_(self, devices):
        dev_info = []
        for device in devices:
            d_formula = device['m']['bee__calculationFormula']
            d_formula = f"<root>{d_formula}</root>"
            formula_tree = ElementTree.fromstring(d_formula)
            dev_info.extend(formula_tree.findall("./mh"))
        devices_uris = [base64.b64decode(x.text.encode()).decode() for x in dev_info]
        driver = neo4j.GraphDatabase.driver(**self.neo4j_connection)
        measurement_list = driver.session().run(f"""
            MATCH (n:saref__Device)-[:saref__makesMeasurement]->(m)-[:saref__relatesToProperty]->(p:saref__Property)
            WHERE n.uri in {devices_uris}
            RETURN n.uri as uri, collect({{hash: m.bigg__hash , freq: m.bigg__measurementFrequency, func: p.bigg__aggregationFunction}}) as mes
            UNION MATCH(n:s4city__KeyPerformanceIndicatorAssessment)-[:s4city__quantifiesKPI]->(q:s4city__KeyPerformanceIndicator)
            WHERE n.uri in {devices_uris}
            RETURN n.uri as uri, collect({{hash: n.bigg__hash , freq: n.bigg__measurementFrequency, func: q.bigg__aggregationFunction}}) as mes
            """).data()
        return {x['uri']: x['mes'] for x in measurement_list}

    def _get_prio_(self, device, devices, dev_info):
        if 'prio' in device:
            return device['prio']
        else:
            self._calc_prio_(device, devices, dev_info)
            return device['prio']

    def _calc_prio_(self, device, devices, dev_info):
        if 'prio' in device:
            return
        d_formula = device['m']['bee__calculationFormula']
        d_formula = f"<root>{d_formula}</root>"
        formula_tree = ElementTree.fromstring(d_formula)
        device_prio = 0
        for d_dep in formula_tree.findall("./mh"):
            try:
                measurement_list = dev_info[base64.b64decode(d_dep.text.encode()).decode()]
                for dep_device in devices:
                    if any([dep_device['m']['bigg__hash'] == x['hash'] for x in measurement_list]):
                        device_prio = max(device_prio, self._get_prio_(dep_device, devices, dev_info)+1)
                        break
            except:
                device_prio = 0
        device['prio'] = device_prio

    def order_by_dependencies(self, devices):
        dev_info = self._get_device_measurements_(devices)
        for device in devices:
            self._calc_prio_(device, devices, dev_info)
        priority_dev = {}
        for d in devices:
            try:
                priority_dev[d['prio']].append(d)
            except KeyError:
                priority_dev[d['prio']] = [d]
        return priority_dev

    def __clip__(self, ts_ini, ts_end, freq, **kwargs):
        df_min = self.calculate_formula(kwargs['op0'], ts_ini, ts_end, freq, level="root", custom_df=None)
        df_max = self.calculate_formula(kwargs['op1'], ts_ini, ts_end, freq, level="root", custom_df=None)
        df_function = self.calculate_formula(kwargs['op2'], ts_ini, ts_end, freq, level="root", custom_df=None)
        df_function['value'] = df_function.value.clip(lower=df_min['value'], upper=df_max['value'])
        return df_function[['value']]

    def __he__(self, ts_ini, ts_end, freq, **kwargs):
        new_ts_ini = ts_end - datetime.timedelta(days=180)
        max_summer = self.calculate_formula(kwargs['op0'], new_ts_ini, ts_end, freq, level="root", custom_df=None)
        min_winter = self.calculate_formula(kwargs['op1'], new_ts_ini, ts_end, freq, level="root", custom_df=None)
        df_parent = self.calculate_formula(kwargs['op2'], new_ts_ini, ts_end, freq, level="root", custom_df=None)
        common_indexes = max_summer.index.intersection(min_winter.index).intersection(df_parent.index)
        max_summer = max_summer.loc[common_indexes]
        min_winter = min_winter.loc[common_indexes]
        df_parent = df_parent.loc[common_indexes]
        df_he = pd.DataFrame(index=df_parent.index, columns=['value'])
        for i in range(len(df_parent)):
            if df_parent['value'].iloc[i] < max_summer['value'].iloc[i]:
                df_he['value'].iloc[i] = 1
            elif df_parent['value'].iloc[i] > min_winter['value'].iloc[i]:
                df_he['value'].iloc[i] = 0
            else:  # Valor entre 20 y 30
                if i == 0:  # Primera fila
                    df_he['value'].iloc[i] = float('nan')
                else:
                    df_he['value'].iloc[i] = df_he['value'].iloc[i - 1]

        df_he = df_he[ts_ini:ts_end]
        return df_he[['value']]


    def __absolute__(self, ts_ini, ts_end, freq, **kwargs):
        df_data = self.calculate_formula(kwargs['op0'], ts_ini, ts_end, freq, level="root", custom_df=None)
        return df_data[['value']].abs()

    def __get_timeseries__(self, bigg_hash, source, ts_ini, ts_end, freq):
        device_uri = base64.b64decode(bigg_hash.encode()).decode()
        driver = neo4j.GraphDatabase.driver(**self.neo4j_connection)
        hash_list = driver.session().run(f"""
            MATCH (n:saref__Device)-[:saref__makesMeasurement]->(m)-[:saref__relatesToProperty]->(p:saref__Property)
            WHERE n.uri="{device_uri}" 
            RETURN m.bigg__hash as hash, m.bigg__measurementFrequency as freq, p.bigg__aggregationFunction as func
            UNION MATCH(n:s4city__KeyPerformanceIndicatorAssessment)-[:s4city__quantifiesKPI]->(q:s4city__KeyPerformanceIndicator)
            WHERE n.uri="{device_uri}" 
            RETURN n.bigg__hash as hash, n.bigg__measurementFrequency as freq, q.bigg__aggregationFunction as func
            """).data()
        ts_ini = isodate_floor(pd.Timestamp(ts_ini), freq)
        ts_end = isodate_floor(pd.Timestamp(ts_end), freq)
        device_df = pd.DataFrame(index=pd.date_range(start=ts_ini, end=ts_end,
                                                     freq=transform_freq(freq)))
        for h in sorted(hash_list,
                        key=lambda x: abs(comparable_freq(x['freq']) - comparable_freq(freq)),
                        reverse=False):
            if "druid" in source:
                tmp = beelib.beedruid.get_timeseries_from_druid(h['hash'], self.druid_connection,
                                                                self.druid_datasource, ts_ini, ts_end)

            elif "influx" in source:
                tmp = beelib.beeinflux.get_timeseries_by_hash(h['hash'], h['freq'], self.influx_connection,
                                                              ts_ini, ts_end)
            else:
                raise Exception("No source database found")
            if tmp.empty:
                continue
            if h['func'] not in self.OPERATION_MAP.keys():
                return None
            if comparable_freq(freq) > comparable_freq(h['freq']):
                # downsample
                device_df['value'] = (tmp.value.astype(float).round(5).resample(transform_freq(freq)).
                                      agg([self.OPERATION_MAP[h["func"]]]))
            elif comparable_freq(freq) < comparable_freq(h['freq']):
                # upsample
                device_df['value'] = tmp.value.astype(float).round(5).resample(transform_freq(freq)).ffill()
                if h["func"] in ["SUM"]:
                    group = device_df.apply(lambda x: tmp[tmp.index <= x.name].index.max(),
                                                         axis=1)
                    count = device_df.groupby(group).size().resample(transform_freq(freq)).ffill()
                    device_df['value'] = (device_df['value'] / count.astype(float))
            else:
                device_df['value'] = tmp.value.astype(float).round(5)
            return device_df[['value']].astype(float)
        device_df['value'] = np.nan
        return device_df

    def __create_constant_timeseries__(self, value, ts_ini, ts_end, freq):
        ts_ini = isodate_floor(pd.Timestamp(ts_ini), freq)
        ts_end = isodate_floor(pd.Timestamp(ts_end), freq)
        r = pd.date_range(ts_ini, ts_end, freq=transform_freq(freq))
        return pd.DataFrame({"value": value}, index=r).astype(float)

    def __create_query_timeseries__(self, encoded_query, ts_ini, ts_end, freq):
        driver = neo4j.GraphDatabase.driver(**self.neo4j_connection)
        query = base64.b64decode(encoded_query.encode()).decode('utf-8')
        value = list(driver.session().run(query).data()[0].values())[0]
        return self.__create_constant_timeseries__(value, ts_ini, ts_end, freq)

    def __apply_function__(self, formula_tree, ts_ini, ts_end, freq, level, custom_df):
        func_imp = self.AVAILABLE_FUNCTIONS[formula_tree.find("./fop").text]
        params = {}
        for x, fpar in enumerate(formula_tree.findall("./fpar")):
            params[f'op{x}'] = fpar
        custom_df[f"{level}"] = func_imp(ts_ini, ts_end, freq, **params)
        new = ElementTree.Element("custom_df")
        new.text = f"{level}"
        return formula_tree, custom_df

    def __apply_operation__(self, formula_tree, op, ts_ini, ts_end, freq, level, custom_df):
        index = list(formula_tree).index(op)
        op1 = list(formula_tree)[index - 1]
        op2 = list(formula_tree)[index + 1]
        formula_tree.remove(op)
        formula_tree.remove(op1)
        formula_tree.remove(op2)
        op1_v = self.calculate_formula(op1, ts_ini, ts_end, freq, f"{level}_{index}", custom_df)
        op2_v = self.calculate_formula(op2, ts_ini, ts_end, freq, f"{level}_{index}", custom_df)
        if op1_v.empty or op2_v.empty:
            raise Exception("Empty series")
        custom_df[f"{level}_{index}"] = eval(f"op1_v.astype(float) {op.text} op2_v.astype(float)")
        new = ElementTree.Element("custom_df")
        new.text = f"{level}_{index}"
        formula_tree.insert(index - 1, new)
        return formula_tree, custom_df

    def calculate_formula(self, formula_tree, ts_ini, ts_end, freq, level="root", custom_df=None):
        if custom_df is None:
            custom_df = {}
        if len(formula_tree) == 0:
            if formula_tree.tag == 'custom_df':
                return custom_df.pop(formula_tree.text)
            elif formula_tree.tag == 'mv':
                return self.__create_constant_timeseries__(formula_tree.text, ts_ini, ts_end, freq)
            elif formula_tree.tag == 'mc':
                return self.__create_constant_timeseries__(self.KPIS_FACTORS[formula_tree.text], ts_ini, ts_end, freq)
            elif formula_tree.tag == 'mq':
                return self.__create_query_timeseries__(formula_tree.text, ts_ini, ts_end, freq)
            elif formula_tree.tag == 'mh':
                return self.__get_timeseries__(formula_tree.text, self.source, ts_ini, ts_end, freq)
        else:
            if formula_tree.tag == 'fun':
                formula_tree, custom_df = self.__apply_function__(formula_tree, ts_ini, ts_end, freq, level,
                                                                  custom_df)
            if len(formula_tree) == 1:
                return self.calculate_formula(formula_tree[0], ts_ini, ts_end, freq, level, custom_df)
            else:
                priority_op = formula_tree.findall("./mo[.='*']") + formula_tree.findall("./mo[.='/']")
                for op in priority_op:
                    formula_tree, custom_df = self.__apply_operation__(formula_tree, op, ts_ini, ts_end, freq, level,
                                                                       custom_df)
                for op in formula_tree.findall("./mo"):
                    formula_tree, custom_df = self.__apply_operation__(formula_tree, op, ts_ini, ts_end, freq, level,
                                                                       custom_df)
            return custom_df.popitem()[1]

    def get_timeseries_components(self, formula_tree, ts_ini, ts_end, freq):
        df_components = {}
        if len(formula_tree) == 0:
            if formula_tree.tag == 'mv':
                return {formula_tree.text: self.__create_constant_timeseries__(formula_tree.text, ts_ini, ts_end, freq)}
            elif formula_tree.tag == 'mc':
                return {formula_tree.text: self.__create_constant_timeseries__(self.KPIS_FACTORS[formula_tree.text], ts_ini, ts_end, freq)}
            elif formula_tree.tag == 'mq':
                return {formula_tree.text: self.__create_query_timeseries__(formula_tree.text, ts_ini, ts_end, freq)}
            elif formula_tree.tag == 'mh':
                return {formula_tree.text: self.__get_timeseries__(formula_tree.text, self.source, ts_ini, ts_end, freq)}
        else:
            if formula_tree.tag == 'fun':
                for x, fpar in enumerate(formula_tree.findall("./fpar")):
                    df_components.update(self.get_timeseries_components(fpar, ts_ini, ts_end, freq))
            elif len(formula_tree) == 1:
                df_components.update(self.get_timeseries_components(formula_tree[0], ts_ini, ts_end, freq))
            else:
                for op in formula_tree.findall("./mo"):
                    index = list(formula_tree).index(op)
                    op1 = list(formula_tree)[index - 1]
                    op2 = list(formula_tree)[index + 1]
                    df_components.update(self.get_timeseries_components(op1, ts_ini, ts_end, freq))
                    df_components.update(self.get_timeseries_components(op2, ts_ini, ts_end, freq))
            return df_components

