import numpy as np
import pandas as pd


def __max_power_apply__(t, dev): return float(dev['harmonized.max_power']) / (pd.Timedelta("PT1H") / t)


def clean_instant_energy_data(df, dev, clean_column="value"):
    if dev['harmonized.name'] in ["Generation", "ExportedToGrid", "ImportedFromGrid", "Consumption"]:
        if not dev['harmonized.max_power']:
            dev['harmonized.max_power'] = 10000
        df.loc[df['value'] < 0, 'value'] = np.nan
        df = max_power_clean(df, dev, False, clean_column)
        if dev['harmonized.name'] in ["Generation", "ExportedToGrid"]:
            # DETECTEM CONSUMS NOCTURNS
            df = night_consumption_clean(df, dev, False, clean_column="value")
        return df
    return znorm_clean(df, dev)


def clean_modbus_energy_data(df, dev, clean_column="value"):

    if dev['harmonized.name'] in ["Generation", "ExportedToGrid", "ImportedFromGrid", "Consumption"]:
        df["diff"] = df["value"].diff()
        if len(df.loc[df['diff'] != 0]) > 30:
            df.loc[df['diff'] != 0, "diff"] = percentile_clean(df.loc[df['diff'] != 0], dev, "diff")
        else:
            df.loc[:, "diff"] = percentile_clean(df, dev, "diff")
        df = df.dropna(subset=["diff"])
        df[clean_column] = df['diff'].cumsum()
        df.drop("diff", axis=1, inplace=True)
        df = incremental_only_clean(df, dev, clean_column=clean_column)
        if not dev['harmonized.max_power']:
            dev['harmonized.max_power'] = 10000
        if dev['harmonized.name'] in ["Generation", "ExportedToGrid"]:
            df = max_power_clean(df, dev, True, clean_column)
            # DETECTEM CONSUMS NOCTURNS
            df = night_consumption_clean(df, dev, True, clean_column=clean_column)
        return df
    df.loc[df['value'] != 0, "value"] = percentile_clean(df.loc[df['value'] != 0], dev, "value")
    return df


def detect_reset_clean(df, dev, clean_column="value"):
    df['diff_value'] = df[clean_column].diff()
    df['diff_time'] = df.index.diff()
    # DETECTEM UN RESET DE CONTADOR:
    reset_candidate = [df.index[0]] + list(df[(df[clean_column] < __max_power_apply__(df['diff_time'], dev)) &
                                              (df['diff_value'] < 0)].index) + [df.index[-1]]
    return [df.loc[s:e] for s, e in zip(reset_candidate, reset_candidate[1:])]


def max_power_clean(df, dev, accumulated, clean_column="value"):
    if dev['harmonized.max_power']:
        if accumulated:
            df['diff_value'] = df[clean_column].diff()
        else:
            df['diff_value'] = df[clean_column]
        df['diff_time'] = df.index.diff()
        df[clean_column] = df.apply(lambda x:
                                    x[clean_column] if x['diff_value'] < __max_power_apply__(x['diff_time'], dev)
                                    else np.nan,
                                    axis=1)
        df.drop(['diff_value', 'diff_time'], axis=1, inplace=True)
        return df


def incremental_only_clean(df, dev, clean_column="value"):
    df['max_v'] = df[clean_column].cummax()
    df['is_error'] = df[clean_column] < df['max_v']
    df = df[df['is_error'] == False]
    df.drop(['is_error', 'max_v'], axis=1, inplace=True)
    return df


def percentile_clean(df, dev, clean_column="value"):
    q1 = np.nanpercentile(df[clean_column], 10)
    q3 = np.nanpercentile(df[clean_column], 90)
    iqr = max(q3 - q1, 1)
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    df[clean_column] = df[clean_column].apply(
        lambda x: x if lower_bound <= x <= upper_bound else np.nan)
    return df


def night_consumption_clean(df, dev, accumulated, clean_column="value"):
    df['refill'] = df[[clean_column]].apply(lambda x: x.name.hour >= 23 or 0 <= x.name.hour < 6, axis=1)
    if accumulated:
        # canviem a -100 els NA actuals ara per tornar-los a posar despres
        df[clean_column] = df[clean_column].fillna(-100)
        # POSEM EL PRIMER A FALSE PER NO PERDRE DADES
        df.loc[df.index[0], 'refill'] = False
        df.loc[(df['refill'] == True) & (df[clean_column] != -100), clean_column] = np.nan
        df[clean_column] = df[clean_column].fillna(method='ffill')
        df.loc[df[clean_column] == -100, clean_column] = np.nan
    else:
        # posem els valors a 0
        df.loc[df['refill'] == True, clean_column] = 0
    df.drop(['refill'], axis=1, inplace=True)
    return df


def no_clean(df, dev, clean_column="value"):
    return df


def znorm_clean(df, dev, clean_column="value"):
    if dev['harmonized.aggregationFunction'] == "LAST":
        return df
    window = df[df[clean_column].diff() != 0]
    w_size = 50
    # clean outliers of initial window with quantiles
    for i in range(0, len(window)):
        w = window[i:i+w_size]
        w = percentile_clean(w, dev, clean_column)
        window.loc[window.iloc[i].name:window.iloc[i + min(len(w)-1, w_size)].name, clean_column] = w[clean_column]
    window.dropna(subset=[clean_column], inplace=True)
    for i in range(w_size, len(df), 1):
        w = window[:df.iloc[i].name]
        w = w[-w_size:]
        mean = w[clean_column].mean()
        std = w[clean_column].std()
        zscore = abs(df[clean_column][i] - mean) / std
        if zscore > 8:
            df[clean_column][i] = np.nan
    return df[w_size:]
