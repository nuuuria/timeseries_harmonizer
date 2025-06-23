import datetime
import beelib
import load_dotenv
import neo4j
import xml.etree.ElementTree as ElementTree
import pandas as pd
from launcher_v2 import FREQ_CONFIG
from lib2 import send_to_kafka
from lib2.calculate_formulas import CalculateFunctions
from tools.plot_utils import plot_dataframes


calculation_hashes = ["a5e1df16cb562dd411bce5f0712991e4359aca0042ad7b4db03b9e236290984b"]
freq = FREQ_CONFIG["PT15M"]

ts_ini = datetime.datetime.fromisoformat("2025-05-01T10:00:00+00:00")
ts_end = datetime.datetime.fromisoformat("2025-05-01T19:00:00+00:00")

# ts_end = datetime.datetime.now(datetime.timezone.utc)
# ts_ini = ts_end - datetime.timedelta(days=FREQ_CONFIG[args.frequency]['days_to_gather'])

load_dotenv.load_dotenv()
config = beelib.beeconfig.read_config('config_prod.json')
driver = neo4j.GraphDatabase.driver(**config['neo4j'])


calc_query = f"""
                MATCH (m)-[:s4city__quantifiesKPI|:saref__relatesToProperty]->(prop) 
                WHERE m.bee__calculationFormula IS NOT NULL AND m.bigg__measurementFrequency="{freq['freq']}"
                RETURN {{bigg__hash: m.bigg__hash, 
                bee__calculationFormula: m.bee__calculationFormula, 
                bigg__measurementFrequency: m.bigg__measurementFrequency,
                property:prop.uri}} AS m
            """
devices = [d['m'] for d in driver.session().run(calc_query).data()]
devices = [d for d in devices if d['bigg__hash'] in calculation_hashes]

all_results = {}
all_components = {}
for dev in devices:
    formula = dev["bee__calculationFormula"]
    formula = f"<root>{formula}</root>"
    formula_tree = ElementTree.fromstring(formula)
    calculator = CalculateFunctions({}, {}, config['influx'], config['neo4j'],
                                    "influx")
    try:
        components = calculator.get_timeseries_components(formula_tree, ts_ini, ts_end, freq['freq'])
        result = calculator.calculate_formula(formula_tree, ts_ini, ts_end, freq['freq'])
    except Exception as e:
        print(f"error with calculations {e}")
        result = None
        components = None
    all_results[dev['bigg__hash']] = result
    all_components[dev['bigg__hash']] = components

# PLOT CALCULATION
for dev in devices:
    components = all_components[dev['bigg__hash']]
    result = all_results[dev['bigg__hash']]
    df_plots = []
    i = 1
    for _, c in components.items():
        df_plots.append(
            {"title": "component", "row": i, "col": 1,
             "data": [{"df": c, "fields": ["value"]}]
             }
        )
        i += 1
    df_plots.append(
        {"title": "result", "row": i, "col": 1,
         "data": [{"df": result, "fields": ["value"]}
                  ]
         }
    )
    plot_dataframes(df_plots)
