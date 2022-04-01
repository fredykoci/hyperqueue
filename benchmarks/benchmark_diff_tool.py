import time
from time import sleep

from rich.align import Align
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.control import Control
from rich.layout import Layout
import pandas as pd
import json
import click

console = Console()


def pd_to_rich_tables(data, compare, params):
    tables = []
    for i in range(len(list((data.iterrows())))):
        d = data.iloc[i]
        c = compare.iloc[i]
        table = Table(title=str(params[i]))
        headers = list(d.index)
        for i in range(len(d)):
            style = ''
            if c[i]:
                style = ''
            else:
                style = 'red'
            table.add_column(headers[i], style=style)

        row = [str(i) for i in d]
        table.add_row(*row)
        tables.append(table)
    return tables


def format_data(data):
    data_formated = pd.DataFrame(columns=["count", "mean", "std", "min", "max"])
    max_index = data['index'].max() + 1

    for row in range(0, len(data), max_index):
        d = data.iloc[row:row + max_index]
        row = {"count": max_index, "mean": d['duration'].mean(), "std": d['duration'].std(), "min": d['duration'].min(),
               "max": d['duration'].max()}
        data_formated = data_formated.append(row, ignore_index=True)

    data_formated = pd.concat([data_formated, data[data["index"] == 0]['env-params'].reset_index()['env-params']],
                              axis=1)

    return data_formated


@click.command()
@click.argument("data-json1")
@click.argument("data-json2")
def compare(data_json1, data_json2):
    '''
    tool to compare two data.json file - must be from same benchmark
    '''
    data_path = data_json1
    data_path1 = data_json2

    data = json.load(open(data_path))
    data1 = json.load(open(data_path1))

    data = pd.DataFrame(data['data'], columns=data['columns'])
    data1 = pd.DataFrame(data1['data'], columns=data1['columns'])

    data = format_data(data)
    data1 = format_data(data1)
    params = data['env-params']
    data = data.drop(['env-params'], axis=1)
    data1 = data1.drop(['env-params'], axis=1)

    compare_data = data <= data1
    compare_data1 = data1 <= data
    tables1 = pd_to_rich_tables(data, compare_data, params)
    tables2 = pd_to_rich_tables(data1, compare_data1, params)

    layout = Layout(name='main')
    layout['main'].split_row(Layout(name='left'), Layout(name='right'))

    left_layouts = [Layout(name=f"{i}_{id}_left") for id, i in enumerate(params)]
    layout['left'].split_column(*left_layouts)
    for i in range(len(params)):
        layout[f"{params[i]}_{i}_left"].update(tables1[i])

    right_layouts = [Layout(name=f"{i}_{id}_right") for id, i in enumerate(params)]
    layout['right'].split_column(*right_layouts)
    for i in range(len(params)):
        layout[f"{params[i]}_{i}_right"].update(tables2[i])

    with console.screen() as screen:
        screen.update(layout)
        console.control(Control.show_cursor(True))
        console.control(Control.move_to(0, 80))
        console.input(": ")


if __name__ == "__main__":
    compare()
