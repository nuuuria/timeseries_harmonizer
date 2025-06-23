import beelib
import numpy as np
import pandas as pd


def get_hbase_data(hbase_connection, source, dev, row_start=None, row_stop=None, freq=None):
    row_start = f"{dev['raw_data.uri']}~{int(row_start.timestamp())}"
    row_stop = f"{dev['raw_data.uri']}~{int(row_stop.timestamp())}"
    tables = beelib.beehbase.get_tables(source['table'] + freq+"$", hbase_connection[source['hbase']]['connection'])
    df_raw = pd.DataFrame()
    for table in tables:
        table_freq = table.split('_')[-1] if table.split('_')[-1] != 'time' else None
        for data in beelib.beehbase.get_hbase_data_batch(hbase_connection[source['hbase']]['connection'],
                                                         table, row_start=row_start, row_stop=row_stop):
            items = []
            for key, row in data:
                dev_info, ts = key.decode("utf-8").split("~")
                item = {'uri': dev_info, 'ts': int(ts), 'freq': table_freq}
                for k, v in row.items():
                    item[k.decode("utf-8").split(":")[1]] = v.decode("utf")
                items.append(item)
            df_raw = pd.concat([df_raw, pd.DataFrame(items)], ignore_index=True)
    return df_raw


def harmonize_irregular_data(df, agg_func, freq, value_column):
    if agg_func == "SUM":
        remove_neg = df[value_column].diff() < 0
        df = df[~remove_neg]
        df_resampled_total = df[value_column].resample('1s').mean().interpolate(method='linear')
        df_resampled_diff = df_resampled_total.diff()
        df_resampled_diff = df_resampled_diff[df_resampled_diff >= 0]
        df_resampled_count = df_resampled_diff.resample(pd.Timedelta(freq)).count()
        df_resampled_sum = df_resampled_diff.resample(pd.Timedelta(freq)).sum()
        return pd.DataFrame(df_resampled_sum[df_resampled_count == pd.Timedelta(freq).total_seconds()])
    elif agg_func == "AVG":
        df_resampled_total = df[value_column].resample('1s').mean().interpolate(method="linear")
        df_resampled_count = df_resampled_total.resample(pd.Timedelta(freq)).count()
        df_resampled_avg = df_resampled_total.resample(pd.Timedelta(freq)).mean()
        return pd.DataFrame(df_resampled_avg[df_resampled_count == pd.Timedelta(freq).total_seconds()])
    elif agg_func == "LAST":
        return pd.DataFrame(df[value_column].resample(pd.Timedelta(freq)).last().ffill())


def unit_conversion(x):
    val = x['value']
    raw_conv_r = float(x.raw_conv_ratio) if x.raw_conv_ratio and not np.isnan(x.raw_conv_ratio) else 1
    harm_conv_r = float(x.harm_conv_ratio) if x.harm_conv_ratio and not np.isnan(x.harm_conv_ratio) else 1
    raw_conv_o = float(x.raw_conv_offset) if x.raw_conv_offset and not np.isnan(x.raw_conv_offset) else 0
    harm_conv_o = float(x.harm_conv_offset) if x.harm_conv_offset and not np.isnan(x.harm_conv_offset) else 0
    return val * float(raw_conv_r) / float(harm_conv_r) + (float(raw_conv_o) - float(harm_conv_o))


def send_to_kafka(producer, kafka_topic, df_to_send):
    df_list = df_to_send.to_list()
    data = None
    for i in range(0, len(df_list), 500):
        for data in df_list[i:i + 500]:
            producer.send(kafka_topic, data)
        producer.flush()
    return data


def complete_missing_points(df, ts_ini, ts_end, freq):
    ts_ini = pd.Timestamp(ts_ini).floor(pd.Timedelta(freq))
    ts_end = pd.Timestamp(ts_end).floor(pd.Timedelta(freq))
    full_time_index = pd.date_range(start=ts_ini, end=ts_end, freq=pd.Timedelta(freq))
    complete = df.reindex(full_time_index, fill_value=None)
    complete['value'] = complete.value.astype(object)
    complete[pd.isna(complete['value'])] = None
    return complete.reset_index(names="timestamp")
