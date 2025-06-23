from lib2 import get_hbase_data, unit_conversion
from lib2.clean_outliers import no_clean

query_status = """
            MATCH (n:bee__IxonDevice)-[:saref__makesMeasurement]->(m:saref__Measurement)
            WHERE m.bigg__measurementFrequency = '{freq}'
            MATCH (m)-[:saref__isMeasuredIn]->(mmu)
            MATCH (m)-[:saref__relatesToProperty]->(mp)
            WITH m, mmu, mp, {{uu:mmu.uri, raw_unitConversionRatio:mmu.qudt__conversionMultiplier, 
            raw_unitConversionOffset: mmu.qudt__conversionOffset, 
            uri:replace(m.uri, "%2B","+"), freq: coalesce(m.bigg__measurementRawFrequency, "")}} as mm
            RETURN DISTINCT {{property: mp.uri, bigg__hash: m.bigg__hash, 
                              measurementFrequency:m.bigg__measurementFrequency, uu:mmu.uri,
                              harmonized_unitConversionRatio:mmu.qudt__conversionMultiplier, 
                              harmonized_unitConversionOffset: mmu.qudt__conversionOffset,
                              aggregationFunction:mp.bigg__aggregationFunction}} as harmonized, 
                              collect(DISTINCT mm) as raw_data
    """


def get_ixon_value(x, dev):
    x['value'] = int(x.status)
    return int(unit_conversion(x))


ixon = {
    "name": "IxonStatus",
    "device_query": [query_status],
    "raw_data": get_hbase_data,
    "raw_data_args": """{'hbase_connection': hbase_connection , 'source': source_config, 
                         'row_start': ts_ini, 'dev': dev, 
                         'row_stop': ts_end, 'freq': reg_freq}""",
    "table": "ixon:timeseries_id_",
    "hbase": "hbase_infraestructures",
    "value": get_ixon_value,
    "pre_clean": no_clean,
    "post_clean": no_clean
}

