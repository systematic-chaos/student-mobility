#!/usr/bin/python3

"""General query 17

Student Mobility

Javier Fernández-Bravo Peñuela
Universitat Politècnica de València

Average economic grant per European exchange student.

pandas - general_query_17.py
"""

from pandas_job.pandas_task import compute_task
from domain.student import Student, MobilityType
from util.functions import round_half_down

import pandas as pd
from sys import argv

EUROPE = "EU"

def european_economical_grant(students_df):
    students_df['GRANT'] = students_df[['MOBILITYTYPE', 'STUDYGRANT', 'PLACEMENTGRANT']]\
        .apply(compute_economical_grant, axis=1)
    students_df = students_df[students_df['GRANT'].notnull()]
    return pd.Series({ EUROPE: round_half_down(students_df[['GRANT']].mean(), 2) })

def compute_economical_grant(row):
    mt = row['MOBILITYTYPE']
    if mt is MobilityType.STUDY:
        sg = row['STUDYGRANT']
        economical_grant = sg
    elif mt is MobilityType.PLACEMENT:
        pg = row['PLACEMENTGRANT']
        economical_grant = pg
    else:
        economical_grant = None
    return economical_grant

# python3 general_query_17.py ../data/Student_Mobility.csv [out/pandas17-output]
if __name__ == "__main__":
    compute_task(european_economical_grant, Student, argv[1:])
