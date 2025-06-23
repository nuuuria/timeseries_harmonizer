import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
"""
df_mapping = [{"title": "titol1", "row": 1, "col": 1, "data": {"df": raw_data, "fields": ["value"]}},
              {"title": "titol2", "row": 2, "col": 1, "data": {"df": df_device_final, "fields": ["value"]}}]
"""


def get_num_series_dict(chart):
    num_lines = 0
    if isinstance(chart['data'], list):
        for data in chart['data']:
            num_lines += 1 if 'series' in data else 0
            num_lines += len(data['fields']) if 'df' in data else 0
    elif isinstance(chart['data'], dict):
        num_lines += 1 if 'series' in chart['data'] else 0
        num_lines += len(chart['data']['fields']) if 'df' in chart['data'] else 0
    return num_lines


def print_chart(fig, num_trace, colors, data, row, col):
    if 'df' in data:
        for field in data['fields']:
            elem = {'df': data['df'], "field": field, "row": row, "col": col}
            num_trace = print_trace(fig, elem, num_trace, colors)
        return num_trace
    elif 'series' in data:
        elem = {'df': data['series'], "row": row, "col": col}
        return print_trace(fig, elem, num_trace, colors)


def print_trace(fig, data, num_trace, colors ):
    if 'field' in data:
        field = {'field': data['field']} if isinstance(data['field'], str) else data['field']
        df = data['df']
        mode = field.get('mode', 'lines+markers')
        name = field.get('title', field['field'])
        color = field.get('color', colors[num_trace])
        field = field['field']
        fig.add_trace(go.Scatter(x=df.index, y=df[field],
                                 mode=mode, name=name, line=dict(color=color)), row=data['row'], col=data['col'])
    else:
        df = data['df']
        mode = 'lines+markers'
        name = "value"
        color = colors[num_trace]
        fig.add_trace(go.Scatter(x=df.index, y=df,
                                 mode=mode, name=name, line=dict(color=color)), row=data['row'], col=data['col'])
    return num_trace + 1


def plot_dataframes(df_mapping, height=500):
    rows = max(x['row'] for x in df_mapping)
    cols = max(x['col'] for x in df_mapping)
    titles = tuple([x.get('title', f"chart_{i}") for i, x in enumerate(df_mapping)])
    fig = make_subplots(rows=rows, cols=cols, shared_xaxes=True, subplot_titles=titles)
    for chart in df_mapping:
        max_traces = get_num_series_dict(chart)
        colors = ['black'] if max_traces <= 1 else (
            px.colors.sample_colorscale("turbo", [n / (max_traces - 1) for n in range(max_traces)]))
        num_trace = 0
        if isinstance(chart['data'], list):
            for data in chart['data']:
                num_trace = print_chart(fig, num_trace, colors, data, row=chart['row'], col=chart['col'])
        elif isinstance(chart['data'], dict):
            num_trace = print_chart(fig, num_trace, colors, chart['data'],
                                    row=chart['row'], col=chart['col'])
    additional_height = 50  # Alçada addicional per sèrie
    total_height = height + len(df_mapping) * additional_height
    fig.update_layout(
        height=total_height,
    )
    fig.show(open_browser=True)
