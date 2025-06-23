from lib2 import get_hbase_data, unit_conversion
from lib2.clean_outliers import no_clean

device_query = """
        MATCH (n:bigg__Patrimony)<-[:s4agri__isDeployedAtSpace]-(d:s4agri__Deployment)
              <-[:ssn__hasDeployment]-(bsys)-[:s4syst__hasSubSystem*..]
              -(x)-[:saref__makesMeasurement]->(m:saref__Measurement)<-[:owl__sameAs]-(mc)
              <-[:saref__makesMeasurement]-(mcd:bee__AgbarDevice)
        WHERE m.bigg__measurementFrequency = '{freq}'
        MATCH (m)-[:saref__isMeasuredIn]->(mmu)
        MATCH (m)-[:saref__relatesToProperty]->(mp)
        MATCH (mcd:bee__AgbarDevice)-[:bigg__measuresIn]->(dmu)       
        WITH m,mmu, mp, {{raw_unitConversionRatio:dmu.qudt__conversionMultiplier, 
                          raw_unitConversionOffset:dmu.qudt__conversionOffset, uri:mc.uri, 
                          freq: coalesce(mc.bigg__measurementRawFrequency, "")}} as mm
        RETURN  
            DISTINCT {{property: mp.uri, bigg__hash: m.bigg__hash, measurementFrequency:m.bigg__measurementFrequency, 
                      harmonized_unitConversionRatio:mmu.qudt__conversionMultiplier, 
                      harmonized_unitConversionOffset:mmu.qudt__conversionOffset, 
                      aggregationFunction:mp.bigg__aggregationFunction}} as harmonized, collect(mm) as raw_data
        """


def get_agbar_value(x, dev):
    x['value'] = float(x['LECTURA_LITRES'])
    return unit_conversion(x)


agbar = {
    "name": "Agbar",
    "device_query": [device_query],
    "raw_data": get_hbase_data,
    "raw_data_args": """{'hbase_connection': hbase_connection , 'source': source_config, 
                         'row_start': ts_ini, 'dev': dev, 
                         'row_stop': ts_end, 'freq': reg_freq}""",
    "table": "agbar:timeseries_id_",
    "hbase": "hbase_infraestructures",
    "value": get_agbar_value,
    "pre_clean": no_clean,
    "post_clean": no_clean
}

