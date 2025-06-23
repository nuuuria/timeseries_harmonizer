import hashlib
import pandas as pd
from pymodbus.payload import BinaryPayloadDecoder



CO2_FACTOR = 0.00012
HOUSE_FACTOR = 4000
TREES_FACTOR = 80

property_configs = {
    'EnergyGenerationElectricity': {'conv': 3600000, "agg_func": 'SUM'},
    'EnergyGenerationElectricityExportedToGrid': {'conv': 3600000, "agg_func": 'SUM'},
    'EnergyConsumptionElectricityImportedFromGrid': {'conv': 3600000, "agg_func": 'SUM'},
    'DeviceInternalTemperature': {'conv': 1, "agg_func": 'AVG'},
    'ElectricityPowerAC': {'conv': 1000, "agg_func": 'AVG'},
    'DeviceStatus': {'conv': 1, "agg_func": 'LAST'}
}


TABLE_RAW = "ixon:raw_ixon_ts_time_{property}_"
TABLE_HARMONIZED = "icat:harmonized_online_{data_type}_100_{agg}_{freq}_icat"
DEXMA_RAW_TABLE = "dexma:raw_dexma_ts_time_{property}_PT15M_"
DEXMA_PARAMETERS = [
    'EACTIVE',
    'PAENERGY'
    ]


def __tag_field_value__(tag_map, read):
    try:
        value = tag_map[read]
    except KeyError:
        value = 0
    return value


def __bit_field_value__(bit_map, binary):
    codi = 0
    for bit, codi in bit_map.items():
        if binary[bit]:
            break
        else:
            codi = 0
    return codi


def __sma_tf32__(binary):
    tag_map = {35: 10, 455: 12, 303: 9}
    return __tag_field_value__(tag_map, binary.decode_32bit_uint())


def __huawei_dl_bf16__(binary):
    bit_map = {6: 1, 7: 2, 0: 3}
    value_int = binary.decode_16bit_uint()
    bit_value = [int(x) for x in list(reversed('{0:016b}'.format(value_int)))]
    return __bit_field_value__(bit_map, bit_value)


def __huawei_dl_tf16__(binary):
    tag_map = {6: 1, 7: 2, 0: 3}
    return __tag_field_value__(tag_map, binary.decode_16bit_uint())


def __huawei_inv_bf16__(binary):
    bit_map = {0: 4, 4: 5, 5: 6, 6: 7, 7: 8, 8: 9}
    value_int = binary.decode_16bit_uint()
    bit_value = [int(x) for x in list(reversed('{0:016b}'.format(value_int)))]
    return __bit_field_value__(bit_map, bit_value)


def __sw_inv_bf32__(binary):
    bit_map = {1: 9, 2: 4, 4: 4, 9: 10, 10: 10, 13: 2, 5: 11, 11: 13}
    value_int = binary.decode_32bit_uint()
    bit_value = [int(x) for x in list(reversed('{0:032b}'.format(value_int)))]
    return __bit_field_value__(bit_map, bit_value)


def __sw_dl_tf16__(binary):
    tag_map = {0: 9}
    return __tag_field_value__(tag_map, binary.decode_16bit_uint())


def decode_modbus_registers(registers, data_type, bo, wo):
    format_map = {
        "U16": lambda x: x.decode_16bit_uint(),
        "U32": lambda x: x.decode_32bit_uint(),
        "U64": lambda x: x.decode_64bit_uint(),
        "I16": lambda x: x.decode_16bit_int(),
        "I32": lambda x: x.decode_32bit_int(),
        "I64": lambda x: x.decode_64bit_int(),
        "TF32SMAIN": lambda x: __sma_tf32__(x),
        "TF32SMADL": lambda x: __sma_tf32__(x),
        "BF16HWDL": lambda x: __huawei_dl_bf16__(x),
        "TF16HWDL": lambda x: __huawei_dl_tf16__(x),
        "BF16HWIN": lambda x: __huawei_inv_bf16__(x),
        "BF32SWIN": lambda x: __sw_inv_bf32__(x),
        "TF16SWDL": lambda x: __sw_dl_tf16__(x),
    }
    binary = BinaryPayloadDecoder.fromRegisters(registers, byteorder=bo, wordorder=wo)
    return format_map[data_type](binary)


def harmonize_accumulated_values(df):
    remove_neg = df.value_calc.diff() < 0
    df = df[~remove_neg]
    df_resampled_total = df.value_calc.resample('1s').interpolate(method='linear')
    df_resampled_diff = df_resampled_total.diff()
    df_resampled_diff = df_resampled_diff[df_resampled_diff >= 0]
    df_resampled_count = df_resampled_diff.resample('15min').count()
    df_resampled_sum = df_resampled_diff.resample('15min').sum()
    return df_resampled_total, df_resampled_count, df_resampled_sum

def harmonize_instantaneous_values(df):
    df_resampled_total = df.value_calc.resample('1s').mean().interpolate(method="linear")
    df_resampled_count = df_resampled_total.resample('15min').count()
    df_resampled_avg = df_resampled_total.resample('15min').mean()
    return df_resampled_total, df_resampled_count, df_resampled_avg


def harmonize_instantaneous_status(df):
    df_resampled_total = df.value_calc.resample('1s').last().ffill()
    return df_resampled_total, df.value_calc.resample('15min').last().ffill()


def create_calc_value_from_records(df):
    df['value_calc'] = df['value'].astype(float) * df['gain'].astype(float)
    df['value_calc'] = (df['value_calc'] * df['unitConversion'].astype(float)
                        / property_configs[df['property'].unique()[0]]['conv'])
    return df


def create_harmonized_timeseries(data, timestamp_key="timestamp", value_key="value"):
    to_save = {
        "start": int(data[timestamp_key].timestamp()),
        'end': int(data[timestamp_key].timestamp() + pd.Timedelta("PT15M").total_seconds()) - 1,
        'value': data[value_key],
        'isReal': True,
        'hash': str(hashlib.sha256(data['device'].replace("device", "measurement").encode("utf-8")).hexdigest()),
        'bucket': (int(data['timestamp'].timestamp()) // settings.TS_BUCKETS) % settings.BUCKETS
    }
    return to_save


def create_harmonized_kpis(data, timestamp_key, value_key, measurement):
    to_save = {
        "start": int(data[timestamp_key].timestamp()),
        "end": int(data[timestamp_key].timestamp() + pd.Timedelta("PT15M").total_seconds()) - 1,
        "value": data[value_key],
        "isReal": True,
        "hash": measurement,
        "bucket": (int(data['timestamp'].timestamp()) // settings.TS_BUCKETS) % settings.BUCKETS
    }
    return to_save


def create_harmonized_druid(data, timestamp_key, value_key):
    to_save = {
        "start": int(data[timestamp_key].timestamp()),
        "end": int(data[timestamp_key].timestamp() + pd.Timedelta("PT15M").total_seconds()) - 1,
        "value": data[value_key],
        "isReal": True,
        "hash": data['hash'],
        "property": data['property']
    }
    return to_save