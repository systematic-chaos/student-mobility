#!/usr/bin/python3

"""General query 5

Student Mobility

Javier Fernández-Bravo Peñuela
Universitat Politècnica de València

Total exchange students received by each Spanish institution.

pandas - general_query_05.py
"""

from pandas_job.pandas_task import compute_task
from domain.student import Student

import re
from sys import argv

UNI_PATTERN = re.compile(r"([A-Z-]+)([0-9]+)")

SPAIN = "ES"
SPANISH_UNIVERSITY_PREFIX = "E"

def students_received_by_spanish_university(students_df):
    students_at_spanish_university_df = students_df[(students_df['COUNTRYOFHOSTINSTITUTION'].eq(SPAIN))\
        & (students_df['HOSTINSTITUTION'].str.startswith(SPANISH_UNIVERSITY_PREFIX))]\
            [['HOSTINSTITUTION']]
    students_at_spanish_university_df['SPANISH_INSTITUTION'] = students_at_spanish_university_df['HOSTINSTITUTION']\
        .apply(uni_pattern_matcher_udf) #meta=('HOSTINSTITUTION', 'string')
    return students_at_spanish_university_df.groupby(['SPANISH_INSTITUTION']).size().sort_index()

def uni_pattern_matcher_udf(uni):
    uni = uni[1:].lstrip()
    uni_matcher = UNI_PATTERN.search(uni)
    return uni_matcher.group(1) if uni_matcher else uni

# python3 general_query_05.py ../data/Student_Mobility.csv [out/pandas05-output]
if __name__ == "__main__":
    compute_task(students_received_by_spanish_university, Student, argv[1:])

