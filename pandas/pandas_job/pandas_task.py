"""pandas - pandas_job/pandas_task.py

Student Mobility

Javier Fernández-Bravo Peñuela
Universitat Politècnica de València
"""

import os
import pandas as pd
from sys import exit, stdout

CSV_SPLIT = ';'
OUTPUT_FILE = "part-r-0000"
SUCCESS_FILE = "_SUCCESS"
FAILURE_FILE = "_FAILURE"

def compute_task(task_func, bean, args):
    if len(args) == 1:
        input = args
        output = None
    elif len(args) > 1:
        input = args[:-1]
        output = args[-1]
    else:
        print('Usage: python student_mobility <in> [[<in>...] <out>]')
        exit(2)
    
    return launch_task(input, output, task_func, bean.get_converters())

def launch_task(input, output, task_func, converters):
    df = read_dataframe(input, converters)

    success = True
    if task_func:
        try:
            df = task_func(df)
        except Exception as e:
            success = False
            ex = e
    
    if output:
        write_output(df, output, success)
    if success:
        print_output(df)
    else:
        print(ex)
    return df

def read_dataframe(input, converters):
    converters = { index: conversion for index, conversion in enumerate(converters) }
    if isinstance(input, str):
        df = read_csv(input, converters)
    else:
        df = pd.concat([read_csv(i, converters) for i in input],
                        ignore_index=True)
    return df


def read_csv(input_file, converters_dict):
    return pd.read_csv(input_file,
                        sep=CSV_SPLIT,
                        index_col=False,
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
