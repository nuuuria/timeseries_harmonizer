import pandas as pd

old_server = {'host': 'master2.internal', 'port': 9090}
new_server = {'host': 'localhost', 'port': 9090}

sources = {
    "export_modbus": {
        "server": new_server,
        "output": "export_modbus.xlsx",
        "table": "modbus:timeseries_id_",
        "row": "https://icat.cat#measurement-INC-05552-ixon-modbus-10.10.10.50-4-32349-PT15M",
    },
    "import_modbus": {
        "server": new_server,
        "output": "import_modbus.xlsx",
        "table": "modbus:timeseries_id_",
        "row": "https://icat.cat#measurement-INC-05552-ixon-modbus-10.10.10.50-4-32357-PT15M",
    }
}

# "modbus": {
#     "old_tables": "ixon:raw_ixon_ts_id_(?P<p>.*)_",
#     "new_tables": {
#         "modbus:timeseries_time_": ['ts', 'device'],
#         "modbus:timeseries_id_": ['device', 'ts']
#     },
#     "clean": clean_modbus
# },
# "dexma": {
#     "old_server": old_server,
#     "new_server": new_server,
#     "old_tables": "dexma:raw_dexma_ts_id_(?P<p>.*)_PT15M_",
#     "new_tables": {
#         "dexma:timeseries_time_PT15M": ['ts', 'device'],
#         "dexma:timeseries_id_PT15M": ['device', 'ts']
#     },
#     "clean": clean_dexma
# },
# "dexma_1": {
#     "old_server": new_server,
#     "new_server": new_server,
#     "old_tables": "dexma:timeseries_id_PT15M",
#     "new_tables": {
#         "dexma:timeseries1_time_PT15M": ['ts', 'device'],
#         "dexma:timeseries1_id_PT15M": ['device', 'ts']
#     },
#     "clean": clean_dexma_1
# },
# "bacnet": {
#     "old_server": old_server,
#     "new_server": new_server,
#     "old_tables": "bacnet:raw_bacnet_ts_id_(?P<p>.*)_",
#     "new_tables": {
#         "bacnet:timeseries_time_": ['ts', 'device'],
#         "bacnet:timeseries_id_": ['device', 'ts']
#     },
#     "clean": clean_bacnet
# },


