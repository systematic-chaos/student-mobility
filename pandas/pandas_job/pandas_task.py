"""pandas - pandas_job/pandas_task.py

Student Mobility

Javier Fernández-Bravo Peñuela
Universitat Politècnica de València
"""

import os
import pandas as pd
import dask.dataframe as dd
import matplotlib.pyplot as plt
from sys import exit, stdout

CSV_SPLIT = ';'
OUTPUT_FILE = "part-r-0000"
SUCCESS_FILE = "_SUCCESS"
FAILURE_FILE = "_FAILURE"
FIGURE_FILE = "plot"

def compute_task(task_func, bean, args, plot=False, parallelize=False):
    if len(args) == 1:
        input = args
        output = None
    elif len(args) > 1:
        input = args[:-1]
        output = args[-1]
    else:
        print('Usage: python student_mobility <in> [[<in>...] <out>]')
        exit(2)
    
    return launch_task(input, output, task_func, bean.get_converters(), plot, parallelize)

def launch_task(input, output, task_func, converters, plot=False, parallelize=False):
    df = read_dataframe_pandas(input, converters) \
        if not parallelize else read_dataframe_dask(input, converters)

    success = True
    if task_func:
        try:
            df = task_func(df)
            if parallelize:
                df = df.compute()
            if plot:
                if round(df.sum(), 2) > 1:
                    draw_bar_plot(df, plot)
                else:
                    draw_pie_plot(df, plot)
        except Exception as e:
            success = False
            ex = e
    
    if output:
        write_output(df, output, success)
    
    if success:
        print_output(df)
        if plot:
            if output:
                save_figure(output)
            else:
                plt.show()
    else:
        print(ex)

    return df

def read_dataframe_pandas(input, converters):
    converters = { index: conversion for index, conversion in enumerate(converters) }
    if isinstance(input, str):
        df = read_csv_pandas(input, converters)
    else:
        df = pd.concat([ read_csv_pandas(i, converters) for i in input ],
                        ignore_index=True)
    return df

def read_dataframe_dask(input, converters):
    converters = { index: conversion for index, conversion in enumerate(converters) }
    if isinstance(input, str):
        df = read_csv_dask(input, converters)
    else:
        df = dd.multi.concat([ read_csv_dask(i, converters) for i in input ],
                            interleave_partitions=True)
    return df

def read_csv_pandas(input_file, converters_dict):
    return pd.read_csv(input_file,
                        sep=CSV_SPLIT,
                        index_col=False,
                        converters=converters_dict,
                        na_filter=False,
                        low_memory=True)

def read_csv_dask(input_file, converters_dict):
    return dd.read_csv(input_file,
                        sep=CSV_SPLIT,
                        converters=converters_dict,
                        na_filter=False,
                        low_memory=True)

def write_output(df, output_path, success=True):
    output_file = os.path.join(output_path, OUTPUT_FILE)
    success_file = os.path.join(output_path, SUCCESS_FILE)
    failure_file = os.path.join(output_path, FAILURE_FILE)

    # Create new empty output directory, clearing its contents in case it already exists
    if os.path.isfile(output_path):
        os.remove(output_path)
    if os.path.exists(output_path):
        if os.path.exists(output_file):
            os.remove(output_file)
        if os.path.isfile(success_file):
            os.remove(success_file)
        if os.path.isfile(failure_file):
            os.remove(failure_file)
    else:
        os.mkdir(output_path, 0o744)
    
    if success:
        with open(output_file, 'w') as outfile:
            print_output(df, outfile)
    
    os.mknod(success_file if success else failure_file, 0o640)

def print_output(df, outstream=stdout):
    if isinstance(df, pd.DataFrame):
        for row in df.itertuples(index=False):
            outstream.write('{}\n'.format('\t'.join([str(item) for item in row])))
    elif isinstance(df, pd.Series):
        for index, value in df.iteritems():
            outstream.write('{}\t{}\n'.format(index, value))

def draw_bar_plot(series, title=None):
    plt.figure(figsize=(8, 6))
    axis = series.plot.bar(color='tab:blue')
    if title and isinstance(title, str):
        axis.set_title(title)
    axis.xaxis.get_label().set_visible(False)
    axis.set_xticklabels([ label.expandtabs(1) for label in series.index ], \
        fontsize=8, rotation=45)
    axis.grid(visible=True, axis='y', which='major', color='lightgray', linewidth=0.5)
    axis.get_figure().tight_layout()
    return axis

def draw_pie_plot(series, title=None):
    plt.figure(figsize=(6, 6))
    axis = series.plot.pie(colormap='tab10', \
        labels=[ label.expandtabs(1) for label in series.index ])
    if title and isinstance(title, str):
        axis.set_title(title)
    axis.yaxis.get_label().set_visible(False)
    axis.get_figure().tight_layout()
    return axis

def save_figure(fig_path, ext='jpg'):
    plt.savefig(os.path.join(fig_path, FIGURE_FILE + '.' + ext))
