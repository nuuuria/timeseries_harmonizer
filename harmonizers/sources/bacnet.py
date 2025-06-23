import numpy as np

from lib2 import get_hbase_data, unit_conversion
from lib2.clean_outliers import no_clean, znorm_clean

device_query = """
        MATCH (n:bigg__Patrimony)<-[:s4agri__isDeployedAtSpace]-(d:s4agri__Deployment)
                  <-[:ssn__hasDeployment]-(bsys)-[:s4syst__hasSubSystem*..]-
                  >(x)-[:saref__makesMeasurement]->(m:saref__Measurement)<-[:owl__sameAs]-(mc)<-[:saref__makesMeasurement]-(mcd:bee__BacnetDevice)
        WHERE m.bigg__measurementFrequency = '{freq}'
        MATCH (m)-[:saref__isMeasuredIn]->(mmu)
        MATCH (m)-[:saref__relatesToProperty]->(mp)
        MATCH (mcd)-[:bigg__measuresIn]->(dmu)
        WITH m,mmu, mp, {{gain: mcd.bee__gain,
                          raw_unitConversionRatio:dmu.qudt__conversionMultiplier, 
                          raw_unitConversionOffset:dmu.qudt__conversionOffset, uri:mc.uri, 
                          freq: coalesce(mc.bigg__measurementRawFrequency, ""), type:mcd.bee__bacnetType}} as mm
        RETURN  
            DISTINCT {{property: mp.uri, bigg__hash: m.bigg__hash, measurementFrequency:m.bigg__measurementFrequency, 
                      harmonized_unitConversionRatio:mmu.qudt__conversionMultiplier, 
                      harmonized_unitConversionOffset:mmu.qudt__conversionOffset, 
                      aggregationFunction:mp.bigg__aggregationFunction}} as harmonized, collect(mm) as raw_data
        """


def __transform_type__(value, _type):
    try:
        return _type(value)
    except ValueError:
        return np.nan


def get_bacnet_value(x, dev):
    """
    analog -> float
    binary -> string
    multistate -> integer
    """

    if 'analog' in dev['raw_data.type']:
        x['value'] = __transform_type__(x["value"], float)
        return unit_conversion(x)

    elif 'binary' in dev['raw_data.type']:
        x['value'] = __transform_type__(x["value"], str)
        if x.value == 'active':
            return 1
        elif x.value == 'inactive':
            return 0
        else:
            return np.nan

    elif 'multiState' in dev['raw_data.type']:
        x['value'] = __transform_type__(x["value"], int)
        return x.value


bacnet = {
    "name": "Bacnet",
    "device_query": [device_query],
    "raw_data": get_hbase_data,
    "raw_data_args": """{'hbase_connection': hbase_connection , 'source': source_config, 
                         'row_start': ts_ini, 'dev': dev, 
                         'row_stop': ts_end, 'freq': reg_freq}""",
    "table": "bacnet:timeseries_id_",
    "hbase": "hbase_infraestructures",
    "value": get_bacnet_value,
    "pre_clean": znorm_clean,
    "post_clean": no_clean
}

