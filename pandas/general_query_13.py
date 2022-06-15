#!/usr/bin/python3

"""General query 13

Student Mobility

Javier Fernández-Bravo Peñuela
Universitat Politècnica de València

Total of exchange students that stay abroad for the fall semester,
the spring semester, and the whole academic year, respectively.

pandas - general_query_13.py
"""

from pandas_job.pandas_task import compute_task
from domain.student import Student, MobilityType

from enum import Enum
from sys import argv

def study_placement_length(students_df):
    students_df['PERIOD_LENGTH'] = students_df.apply(\
        period_length_udf, axis=1, result_type="expand")
    students_df = students_df.dropna(subset=['PERIOD_LENGTH'])
    return students_df.groupby(['PERIOD_LENGTH']).size().sort_index()

def period_length_udf(row):
    (start_date, length) = study_placement_start_date_length(row)
    period_length = None
    if length and length >= 7:
        period_length = PeriodLength.FULL
    elif start_date is not None:
        if start_date.month > 6:
            period_length = PeriodLength.FIRST if length else PeriodLength.FULL
        else:
            period_length = PeriodLength.SECOND
    return period_length.value if period_length else None

def study_placement_start_date_length(row):
    start_date = None
    length = None
    if row['MOBILITYTYPE'] in [MobilityType.STUDY, MobilityType.PLACEMENT]:
        if row['MOBILITYTYPE'] is MobilityType.STUDY:
            start_date = row['STUDYSTARTDATE']
            length = row['LENGTHSTUDYPERIOD']
        elif row['MOBILITYTYPE'] is MobilityType.PLACEMENT:
            start_date = row['PLACEMENTSTARTDATE']
            length = row['LENGTHWORKPLACEMENT']
    return start_date, length

class PeriodLength(Enum):
    FIRST='1'
    SECOND='2'
    FULL='F'

# python3 general_query_13.py ../data/Student_Mobility.csv [out/pandas13-output]
if __name__ == "__main__":
    compute_task(study_placement_length, Student, argv[1:])
