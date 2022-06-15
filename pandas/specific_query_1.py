#!/usr/bin/python3

"""Specific query 1

Student Mobility

Javier Fernández-Bravo Peñuela
Universitat Politècnica de València

How many students, from the same home country as the 22-aged individual,
whose home university code is "CIUDA-R" and whose host university code
is "S VASTERA", took their Erasmus exchange in the same host university.

pandas - specific_query_1.py
"""

from pandas_job.pandas_task import compute_task
from domain.student import Student

import pandas as pd
from sys import argv

HOME_UNIVERSITY = "CIUDA-R"
HOST_UNIVERSITY = "VASTERA"
AGE = 22

def country_companions(students_df):
    me = students_df.iloc[\
        students_df.index[(students_df['HOMEINSTITUTION'].apply(hi_sub_str).str.startswith(HOME_UNIVERSITY))\
            & (students_df['HOSTINSTITUTION'].apply(hi_sub_str).str.startswith(HOST_UNIVERSITY))\
                & (students_df['AGE'].eq(AGE))][0]]

    companions = students_df[(students_df['COUNTRYOFHOMEINSTITUTION'] == me['COUNTRYOFHOMEINSTITUTION'])\
        & (students_df['COUNTRYOFHOSTINSTITUTION'] == me['COUNTRYOFHOSTINSTITUTION'])]
    return pd.Series({ 'COUNTRY_COMPANIONS': companions.shape[0] })

def hi_sub_str(hi):
    return hi[3:]
    

# python3 specific_query_1.py ../data/Student_Mobility.csv [out/pandas-1-output]
if __name__ == "__main__":
    compute_task(country_companions, Student, argv[1:])
