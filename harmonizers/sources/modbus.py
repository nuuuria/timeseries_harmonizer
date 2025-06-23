import numpy as np

from lib2 import get_hbase_data, unit_conversion
from lib2.clean_outliers import no_clean, clean_modbus_energy_data

device_query = """
    MATCH (n:bigg__Patrimony)<-[:s4agri__isDeployedAtSpace]-(d:s4agri__Deployment)
                  <-[:ssn__hasDeployment]-(bsys)-[:s4syst__hasSubSystem*..]->(x)-[:saref__makesMeasurement]->
                  (m:saref__Measurement)<-[:owl__sameAs]-(mc)<-[:saref__makesMeasurement]-(mcd:bee__ModbusDevice)
            WHERE m.bigg__measurementFrequency = '{freq}'
    OPTIONAL MATCH (af:bee__AssetFeature{{foaf__name:"PotenciaNominal"}})<-[:bee__hasFeature]-
                   (inst:s4bldg__BuildingObject)<-[:owl__sameAs]-(pv:bigg__PhotoVoltaic)-[:s4syst__hasSubSystem*..]-
                   ()-[:saref__makesMeasurement]->(m)
    MATCH (m)-[:saref__isMeasuredIn]->(mmu)
    MATCH (m)-[:saref__relatesToProperty]->(mp)
    MATCH (mcd)-[:bigg__measuresIn]->(dmu)
    WITH m,mmu, mp, af, x, {{gain: mcd.bee__gain,
                          offset: mcd.bee__offset,
                          raw_unitConversionRatio:dmu.qudt__conversionMultiplier, 
                          raw_unitConversionOffset:dmu.qudt__conversionOffset, uri:mc.uri, 
                          freq: coalesce(mc.bigg__measurementRawFrequency, "")}} as mm
    RETURN DISTINCT {{property: mp.uri, bigg__hash: m.bigg__hash, measurementFrequency:m.bigg__measurementFrequency,
                      max_power: af.saref__value, name:  x.foaf__name,
                          harmonized_unitConversionRatio:mmu.qudt__conversionMultiplier, 
                          harmonized_unitConversionOffset:mmu.qudt__conversionOffset, 
                          aggregationFunction:mp.bigg__aggregationFunction}} as harmonized, collect(mm) as raw_data
        """


def get_modbus_value(x, dev):
    if abs(float(x.value)) == float(2147483648):
        return np.nan
    else:
        x['value'] = float(x.value) * float(dev['raw_data.gain']) + float(dev['raw_data.offset'])
        return unit_conversion(x)


modbus = {
    "name": "Modbus",
    "device_query": [device_query],
    "raw_data": get_hbase_data,
    "raw_data_args": """{'hbase_connection': hbase_connection , 'source': source_config, 
                         'row_start': ts_ini, 'dev': dev, 
                         'row_stop': ts_end, 'freq': reg_freq}""",
    "table": "modbus:timeseries_id_",
    "hbase": "hbase_infraestructures",
    "value": get_modbus_value,
    "pre_clean": clean_modbus_energy_data,
    "post_clean": no_clean
}



