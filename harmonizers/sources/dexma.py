from lib2 import get_hbase_data, unit_conversion
from lib2.clean_outliers import no_clean, clean_instant_energy_data

device_query = """
    MATCH (n:bigg__Patrimony)<-[:s4agri__isDeployedAtSpace]-(d:s4agri__Deployment)
                  <-[:ssn__hasDeployment]-(bsys)-[:s4syst__hasSubSystem*..]-
                  >(x)-[:saref__makesMeasurement]->(m:saref__Measurement)<-[:owl__sameAs]-(mc)<-[:saref__makesMeasurement]-(mcd:bee__DexmaDevice)
    WHERE m.bigg__measurementFrequency = '{freq}'
    OPTIONAL MATCH (af:bee__AssetFeature{{foaf__name:"PotenciaNominal"}})<-[:bee__hasFeature]-
               (inst:s4bldg__BuildingObject)<-[:owl__sameAs]-(pv:bigg__PhotoVoltaic)-[:s4syst__hasSubSystem*..]-
               ()-[:saref__makesMeasurement]->(m)
    MATCH (m)-[:saref__isMeasuredIn]->(mmu)
    MATCH (m)-[:saref__relatesToProperty]->(mp)
    MATCH (mcd:bee__DexmaDevice)-[:bigg__measuresIn]->(dmu)
        WITH m,mmu, mp, af, x, {{raw_unitConversionRatio:dmu.qudt__conversionMultiplier, 
                          raw_unitConversionOffset:dmu.qudt__conversionOffset, uri:mc.uri,
                          freq: coalesce(mc.bigg__measurementRawFrequency, "")}} as mm
        RETURN  
            DISTINCT {{property: mp.uri, bigg__hash: m.bigg__hash, measurementFrequency:m.bigg__measurementFrequency, 
                      max_power: af.saref__value, name:  x.foaf__name,
                      harmonized_unitConversionRatio:mmu.qudt__conversionMultiplier, 
                      harmonized_unitConversionOffset:mmu.qudt__conversionOffset, 
                      aggregationFunction:mp.bigg__aggregationFunction}} as harmonized, collect(mm) as raw_data
        """

project_query = """
       MATCH (n:bigg__Patrimony)<-[:s4agri__isDeployedAtSpace]-(d:s4agri__Deployment)
             <-[:ssn__hasDeployment]-(bsys:bigg__BuildingSystem)-[:s4syst__hasSubSystem*..]
             -(device:saref__Device)-[:saref__makesMeasurement]->(m)
       WHERE (m:bigg__ExpectedMeasurement OR m:bigg__TargetMeasurement) AND m.bigg__measurementFrequency = '{freq}'
       MATCH (m)-[:saref__isMeasuredIn]->(mmu)
       MATCH (m)-[:saref__relatesToProperty]->(mp)
       MATCH (m)<-[:owl__sameAs]-(mc)<-[:saref__makesMeasurement]-(mcd:bee__DexmaProjectDevice)-[:bigg__measuresIn]->(dmu)
       WITH device, m, mmu, mp, {{raw_unitConversionRatio:dmu.qudt__conversionMultiplier,
                          raw_unitConversionOffset:dmu.qudt__conversionOffset, uri:mc.uri,
                          freq: coalesce(mc.bigg__measurementRawFrequency, "")}} as mm        
       RETURN  
           DISTINCT {{property: mp.uri, bigg__hash: m.bigg__hash, measurementFrequency:m.bigg__measurementFrequency,
                     name: device.foaf__name,
                     harmonized_unitConversionRatio:mmu.qudt__conversionMultiplier, 
                     harmonized_unitConversionOffset:mmu.qudt__conversionOffset, 
                     aggregationFunction:mp.bigg__aggregationFunction}} as harmonized, collect(mm) as raw_data
   """


def get_dexma_value(x, dev):
    if "v" in x:
        x['value'] = float(x.v)
    if "target" in x:
        x['value'] = float(x.target)
    if "baseline" in x:
        x['value'] = float(x.baseline)
    return unit_conversion(x)


dexma = {
    "name": "Dexma",
    "device_query": [device_query],
    "raw_data": get_hbase_data,
    "raw_data_args": """{'hbase_connection': hbase_connection , 'source': source_config, 
                         'row_start': ts_ini, 'dev': dev, 
                         'row_stop': ts_end, 'freq': reg_freq}""",
    "table": "dexma:timeseries_id_",
    "hbase": "hbase_infraestructures",
    "value": get_dexma_value,
    "pre_clean": no_clean,
    "post_clean": clean_instant_energy_data
}

dexma_projects = {
    "name": "DexmaProjects",
    "device_query": [project_query],
    "raw_data": get_hbase_data,
    "raw_data_args": """{'hbase_connection': hbase_connection , 'source': source_config, 
                         'row_start': ts_ini, 'dev': dev, 
                         'row_stop': ts_end, 'freq': reg_freq}""",
    "table": "dexma:timeseries_id_",
    "hbase": "hbase_infraestructures",
    "value": get_dexma_value,
    "pre_clean": no_clean,
    "post_clean": no_clean,
}
