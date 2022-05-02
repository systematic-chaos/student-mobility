#!/usr/bin/python3

"""General query 14

Student Mobility

Javier Fernández-Bravo Peñuela
Universitat Politècnica de València

Proportion, of those students that initially stay abroad just for the fall semester,
that extend their exchange period to the next spring semester.

pandas - general_query_14.py
"""

from pandas_job.pandas_task import compute_task
from domain.student import Student, MobilityType
from util.functions import round_half_down

from sys import argv

def placement_extension_proportion(students_df):
    students_df['PLACEMENT_EXTENSION'] = students_df.apply(\
        placement_extension_udf, axis=1, result_type="expand")
    student_count = students_df.shape[0]
    return students_df.groupby(['PLACEMENT_EXTENSION']).size().sort_index()\
        .apply(lambda pe: round_half_down(pe / student_count))

def placement_extension_udf(row):
    (start_date, length) = study_placement_start_date_length(row)
    placement_extension = length and length >= 7 and start_date is not None and start_date.month > 6
    return 'Y' if placement_extension else 'N'

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

# python3 general_query_14.py ../data/Student_Mobility.csv [out/pandas14-output]
if __name__ == "__main__":
    compute_task(placement_extension_proportion, Student, argv[1:])
