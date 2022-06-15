#!/usr/bin/python3

"""General query 4

Student Mobility

Javier Fernández-Bravo Peñuela
Universitat Politècnica de València

Total exchange students sent by each Spanish institution.

pandas - general_query_04.py
"""

from pandas_job.pandas_task import compute_task
from domain.student import Student

import re
from sys import argv

UNI_PATTERN = re.compile(r"([A-Z-]+)([0-9]+)")

SPAIN = "ES"
SPANISH_UNIVERSITY_PREFIX = "E"

def students_sent_by_spanish_university(students_df):
    spanish_university_students_df = students_df[(students_df['COUNTRYOFHOMEINSTITUTION'].eq(SPAIN))\
        & (students_df['HOMEINSTITUTION'].str.startswith(SPANISH_UNIVERSITY_PREFIX))]\
            [['HOMEINSTITUTION']]
    spanish_university_students_df['SPANISH_INSTITUTION'] = spanish_university_students_df['HOMEINSTITUTION']\
        .apply(uni_pattern_matcher_udf) #meta=('HOMEINSTITUTION', 'string')
    return spanish_university_students_df.groupby(['SPANISH_INSTITUTION']).size().sort_index()

def uni_pattern_matcher_udf(uni):
    uni = uni[1:].lstrip()
    uni_matcher = UNI_PATTERN.search(uni)
    return uni_matcher.group(1) if uni_matcher else uni

# python3 general_query_04.py ../data/Student_Mobility.csv [out/pandas04-output]
if __name__ == "__main__":
    compute_task(students_sent_by_spanish_university, Student, argv[1:])
