#!/usr/bin/python3

"""General query 18

Student Mobility

Javier Fernández-Bravo Peñuela
Universitat Politècnica de València

Average economic grant per Spanish exchange student.

pandas - general_query_18.py
"""

from pandas_job.pandas_task import compute_task
from domain.student import Student, MobilityType
from util.functions import round_half_down

import pandas as pd
from sys import argv

SPAIN = "ES"

def spanish_economical_grant(students_df):
    spanish_students_df = students_df[students_df['COUNTRYOFHOMEINSTITUTION'] == SPAIN].copy()
    spanish_students_df.loc[:, 'GRANT'] = spanish_students_df[['MOBILITYTYPE', 'STUDYGRANT', 'PLACEMENTGRANT']]\
        .apply(compute_economical_grant, axis=1)
    spanish_students_df = spanish_students_df[spanish_students_df['GRANT'].notnull()]
    return pd.Series({ SPAIN: round_half_down(spanish_students_df[['GRANT']].mean(), 2) })

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

# python3 general_query_18.py ../data/Student_Mobility.csv [out/pandas18-output]
if __name__ == "__main__":
    compute_task(spanish_economical_grant, Student, argv[1:])
