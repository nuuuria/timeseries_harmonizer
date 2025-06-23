import pandas as pd
import pytz
from beemeteo.sources.meteogalicia import MeteoGalicia

from lib2 import unit_conversion
from lib2.clean_outliers import no_clean

query_stations = """
            MATCH (n:bigg__Patrimony)<-[:s4agri__isDeployedAtSpace]-(d:s4agri__Deployment)
                  <-[:ssn__hasDeployment]-(bsys)-[:s4syst__hasSubSystem*..]->(x)-[:saref__makesMeasurement]->
                  (m:saref__Measurement)<-[:owl__sameAs]-(mc)<-[:saref__makesMeasurement]-(mcd)
                  <-[:s4syst__hasSubSystem]-(ws:bee__MeteoGalicia)
            WHERE m.bigg__measurementFrequency = '{freq}'
            MATCH (m)-[:saref__isMeasuredIn]->(mmu)
            MATCH (m)-[:saref__relatesToProperty]->(mp)
            MATCH (mcd)-[:bigg__measuresIn]->(dmu)
            WITH m,mmu, mp, {{uu:dmu.uri, raw_unitConversionRatio:dmu.qudt__conversionMultiplier, 
                              raw_unitConversionOffset: dmu.qudt__conversionOffset, 
                              uri:ws.geo__latitude + "~" + ws.geo__longitude,
                              lat: ws.geo__latitude, lon: ws.geo__longitude, 
                              freq: coalesce(mc.bigg__measurementRawFrequency, "")}} as mm
            RETURN  
                DISTINCT {{property: mp.uri, bigg__hash: m.bigg__hash, 
                           measurementFrequency:m.bigg__measurementFrequency, uu:mmu.uri,
                          harmonized_unitConversionRatio:mmu.qudt__conversionMultiplier, 
                          harmonized_unitConversionOffset: mmu.qudt__conversionOffset,
                          aggregationFunction:mp.bigg__aggregationFunction}} as harmonized, 
                        collect(DISTINCT mm) as raw_data
    """


def wm2_to_whm2(x):
    x['raw_conv_ratio'] = 3600
    return x


def get_meteogalicia_value(x, dev):
    property_map = {
        "Temperature": {"field": "airTemperature", "conv": None},
        "GlobalNormalRadiation": {"field": "GHI", "conv": wm2_to_whm2},
        "Pressure": {"field": "atmosphericPressure", "conv": None}
    }
    x['value'] = float(x[property_map[x['property']]['field']])
    if property_map[x['property']]['conv']:
        x = property_map[x['property']]['conv'](x)
    return unit_conversion(x)


def get_meteogalicia(dev, ts_ini, ts_end, source_config, hbase_connection):
    mg_client = MeteoGalicia({"hbase_weather_data": hbase_connection[source_config['hbase']]['connection']})
    data_final = pd.DataFrame()
    data = mg_client.get_forecasting_data(latitude=float(dev['raw_data.lat']),
                                          longitude=float(dev['raw_data.lon']),
                                          date_from=ts_ini, date_to=ts_end, grid=True)
    data['timestamp'] = data['timestamp'].dt.tz_convert(pytz.UTC)
    data['ts'] = data['timestamp'].astype(int).div(10**9)
    data['uri'] = f"{dev['raw_data.lat']}~{dev['raw_data.lon']}"
    data['freq'] = "PT1H"
    data_final = pd.concat([data_final, data])
    return data_final


meteogalicia = {
    "name": "Meteogalicia",
    "device_query": [query_stations],
    "raw_data": get_meteogalicia,
    "raw_data_args": """{'ts_ini': ts_ini, 'ts_end': ts_end, 'dev': dev,
                     'source_config': source_config, 'hbase_connection': hbase_connection}""",
    "hbase": "hbase_meteo",
    "value": get_meteogalicia_value,
    "pre_clean": no_clean,
    "post_clean": no_clean
}

