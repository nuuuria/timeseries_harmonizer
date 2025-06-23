import beelib
import neo4j
import numpy as np
import pandas as pd

from harmonizers.post_process import Processor
from lib2 import send_to_kafka, complete_missing_points


class PVProcessor(Processor):
    name = "PVProcessor"

    @staticmethod
    def get_devices(freq):
        return f"""
                Match(n:bigg__Patrimony)<-[:s4agri__isDeployedAtSpace]-(dp)<-[:ssn__hasDeployment]-(s:bigg__BuildingSystem)-
                [:s4syst__hasSubSystem*..4]->(d:saref__Device)-[:saref__makesMeasurement]->(m:saref__Measurement)
                WHERE m.bigg__measurementFrequency="{freq['freq']}"
                Match(d)-[:saref__measuresProperty]->(p:saref__Property)
                Where  dp.bigg__deploymentType="Electric" with n.bigg__idFromOrganization AS patrimony, 
                [m in collect({{hash:m.bigg__hash, name:d.foaf__name, prop: p.uri, 
                freq:m.bigg__measurementFrequency}})] AS devices return patrimony, devices
            """

    @staticmethod
    def _apply_processor(df_gen, df_exp):
        if df_gen.empty or df_exp.empty:
            return False, None
        df_total = pd.DataFrame()
        df_total['gen'] = df_gen.value.astype(float).round(5)
        df_total['exp'] = df_exp.value.astype(float).round(5)

        problem_index = df_total[(df_total.gen - df_total.exp) < 0].index
        update = False
        for i in problem_index:
            rollback = df_total['exp'].copy(deep=True)
            update = True
            index_num = df_total.index.get_loc(i)
            over_exported = df_total.iloc[index_num].exp - df_total.iloc[index_num].gen
            if over_exported <= 0:
                continue
            df_total.loc[df_total.iloc[index_num].name, 'exp'] = df_total.iloc[index_num]['gen']
            ind = 0
            sign = []
            while over_exported > 0:
                if not sign and ind < 5:
                    ind += 1
                    if (index_num + ind < len(df_total)) and (index_num - ind > 0):
                        sign = [np.negative, np.positive]
                    elif (index_num + ind > len(df_total)) and (index_num - ind > 0):
                        sign = [np.negative]
                    elif (index_num - ind < 0) and (index_num + ind < len(df_total)):
                        sign = [np.positive]
                if not sign:
                    df_total['exp'] = rollback
                    df_total.loc[df_total.iloc[index_num].name, 'exp'] = None
                    break
                current_ind = sign.pop()(ind)
                ex = df_total.iloc[index_num + current_ind]
                m_exp = ex.gen - ex.exp
                if m_exp > 0:
                    current_exp = min(over_exported, m_exp)
                    df_total.loc[df_total.iloc[index_num + current_ind].name, 'exp'] += current_exp
                    over_exported -= current_exp
                else:
                    df_total.loc[ex.name, 'exp'] = ex.gen
                    over_exported += -m_exp
        return update, df_total[['exp']].rename(columns={'exp': 'value'})

    @staticmethod
    def process_device(pv_system, druid_producer, druid_connection, druid_datasource, druid_topic, influx_connection,
                       ts_ini, ts_end, freq):
        try:
            dev = [x for x in pv_system['devices'] if x['name'] == 'ExportedToGrid'][0]
            gen = [x for x in pv_system['devices'] if x['name'] == 'Generation'][0]['hash']
            exp = [x for x in pv_system['devices'] if x['name'] == 'ExportedToGrid'][0]['hash']
        except IndexError:
            return
        if "druid" in druid_topic:
            df_gen = beelib.beedruid.get_timeseries_from_druid(gen, druid_connection,
                                                               druid_datasource, ts_ini, ts_end)
            df_exp = beelib.beedruid.get_timeseries_from_druid(exp, druid_connection,
                                                               druid_datasource, ts_ini, ts_end)
        elif "influx" in druid_topic:
            df_gen = beelib.beeinflux.get_timeseries_by_hash(gen, freq['freq'], influx_connection, ts_ini, ts_end)
            df_exp = beelib.beeinflux.get_timeseries_by_hash(exp, freq['freq'], influx_connection, ts_ini, ts_end)
        else:
            raise Exception("No source database found")
        update, df_processed = PVProcessor._apply_processor(df_gen, df_exp)
        data = None
        if update:
            df_processed = complete_missing_points(df_processed, ts_ini, ts_end, freq['freq'])

            df_processed['hash'] = dev['hash']
            df_processed['property'] = (dev['prop'].replace("https://bigg-project.eu/ontology#", "").
                                        replace("https://saref.etsi.org/core/", ""))
            df_to_save = df_processed.reset_index().apply(
                beelib.beedruid.harmonize_for_druid, timestamp_key="timestamp", value_key="value", hash_key="hash",
                property_key="property", is_real=True,
                freq=dev['freq'],
                axis=1)
            data = send_to_kafka(druid_producer, druid_topic, df_to_save)
        return data
