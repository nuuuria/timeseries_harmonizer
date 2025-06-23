import datetime

import numpy as np
import pytz

from lib2 import unit_conversion, get_hbase_data
import beelib
import pandas as pd

from lib2.clean_outliers import no_clean

indicators_query = """
    WITH ["https://www.beegroup-cimne.com/bee/ontology#TR", "https://www.beegroup-cimne.com/bee/ontology#IN", "https://www.beegroup-cimne.com/bee/ontology#RS"] as uri_eco
    MATCH (n:s4city__KeyPerformanceIndicatorAssessment)-[:s4city__quantifiesKPI]->(im:bee__IndicatorManttest)
    WHERE not im.uri in uri_eco and n.bigg__measurementFrequency = '{freq}' and n.bigg__measurementRawFrequency is not null
    MATCH (n)-[saref__isMeasuredIn]->(u:qudt__Unit)
    WITH n, im, u, {{
    raw_unitConversionRatio:u.qudt__conversionMultiplier, 
    raw_unitConversionOffset:u.qudt__conversionOffset, 
    uri:n.uri, 
    freq: n.bigg__measurementRawFrequency}} as mm
        RETURN DISTINCT {{property: im.uri, 
    bigg__hash: n.bigg__hash,
    measurementFrequency:n.bigg__measurementFrequency,
    harmonized_unitConversionRatio:u.qudt__conversionMultiplier,
    harmonized_unitConversionOffset:u.qudt__conversionOffset, 
    aggregationFunction:im.bigg__aggregationFunction
    }} as harmonized, collect(mm) as raw_data
    """

indicators_eco = """
    WITH ["https://www.beegroup-cimne.com/bee/ontology#TR", "https://www.beegroup-cimne.com/bee/ontology#IN", "https://www.beegroup-cimne.com/bee/ontology#RS"] as uri_eco
    MATCH (n:s4city__KeyPerformanceIndicatorAssessment)-[:s4city__quantifiesKPI]->(im:bee__IndicatorManttest)
    WHERE im.uri in uri_eco and n.bigg__measurementFrequency = '{freq}' and n.bigg__measurementRawFrequency is not null
    MATCH (n)-[saref__isMeasuredIn]->(u:qudt__Unit)
    WITH n, im, u, {{
    raw_unitConversionRatio:u.qudt__conversionMultiplier, 
    raw_unitConversionOffset:u.qudt__conversionOffset, 
    uri:n.uri, 
    freq: n.bigg__measurementRawFrequency}} as mm
        RETURN DISTINCT {{property: im.uri, 
    bigg__hash: n.bigg__hash,
    measurementFrequency:n.bigg__measurementFrequency,
    harmonized_unitConversionRatio:u.qudt__conversionMultiplier,
    harmonized_unitConversionOffset:u.qudt__conversionOffset, 
    aggregationFunction:im.bigg__aggregationFunction
    }} as harmonized, collect(mm) as raw_data
    """


def get_manttest_value(x, dev):
    try:
        if "value" in x:
            x['value'] = float(x.value)
            return unit_conversion(x)
        elif "import" in x:
            x['value'] = float(x['import'])
            return unit_conversion(x)
        else:
            return np.nan
    except AttributeError as e:
        return np.nan


def get_hbase_data_manttest(hbase_connection, source, row_start, row_stop, dev, freq):
    r_data = get_hbase_data(hbase_connection=hbase_connection, source=source, row_start=row_start, row_stop=row_stop,
                            dev=dev, freq=freq)
    if r_data.empty:
        return r_data
    r_data['ts'] = pd.to_datetime(r_data.ts, unit="s").dt.tz_localize("UTC").dt.tz_convert("Europe/Madrid")
    r_data['ts'] = r_data['ts'].apply(lambda x: x.replace(hour=0, minute=0, second=0, microsecond=0, day=1)) - pd.DateOffset(months=1)
    r_data['ts'] = r_data['ts'].astype(int) // 10 ** 9
    return r_data


def remove_month_duplicates(df, dev, clean_column="value"):
    df['month'] = df.index.month.astype(str) + "~" + df.index.year.astype(str)
    df.drop_duplicates(subset="month", keep="last", inplace=True)
    df.drop(columns=['month'], inplace=True)
    return df


manttest = {
    "name": "ManttesIndicators",
    "device_query": [indicators_query],
    "raw_data": get_hbase_data_manttest,
    "raw_data_args": """{'hbase_connection': hbase_connection , 'source': source_config, 
                         'row_start': ts_ini, 'dev': dev, 
                         'row_stop': ts_end, 'freq': reg_freq}""",
    "table": "manttest:timeseries_id_findindicators_",
    "hbase": "hbase_infraestructures",
    "value": get_manttest_value,
    "pre_clean": remove_month_duplicates,
    "post_clean": no_clean
}

manttest_eco = {
    "name": "ManttesIndicatorsEco",
    "device_query": [indicators_eco],
    "raw_data": get_hbase_data,
    "raw_data_args": """{'hbase_connection': hbase_connection , 'source': source_config, 
                         'row_start': ts_ini, 'dev': dev, 
                         'row_stop': ts_end, 'freq': reg_freq}""",
    "table": "manttest:timeseries_id_economic_",
    "hbase": "hbase_infraestructures",
    "value": get_manttest_value,
    "pre_clean": no_clean,
    "post_clean": no_clean
}
