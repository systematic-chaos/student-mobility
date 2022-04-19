#!/usr/bin/python3

"""General query 11

Student Mobility

Javier Fernández-Bravo Peñuela
Universitat Politècnica de València

Gender proportion of Spanish exchange students.

pandas - general_query_11.py
"""

from pandas_job.pandas_task import compute_task
from domain.student import Student
from util.functions import round_half_down

from sys import argv

SPAIN = "ES"

def spanish_gender_proportion(students_df):
    spanish_students_df = students_df[students_df['COUNTRYOFHOMEINSTITUTION'] == SPAIN].copy()
    spanish_students_df.loc[:, 'GENDER'] = spanish_students_df['GENDER'].apply(lambda g: g.value)
    student_count = spanish_students_df.shape[0]
    return spanish_students_df[['GENDER']].groupby(['GENDER']).size().sort_index()\
        .apply(lambda g: round_half_down(g / student_count))

# python3 general_query_11.py ../data/Student_Mobility.csv [out/pandas11-output]
if __name__ == "__main__":
    compute_task(spanish_gender_proportion, Student, argv[1:])
