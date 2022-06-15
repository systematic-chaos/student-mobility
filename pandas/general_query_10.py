#!/usr/bin/python3

"""General query 10

Student Mobility

Javier Fernández-Bravo Peñuela
Universitat Politècnica de València

Gender proportion of European exchange students.

pandas - general_query_10.py
"""

from pandas_job.pandas_task import compute_task
from domain.student import Student
from util.functions import round_half_down

from sys import argv

def gender_proportion(students_df):
    students_df['GENDER'] = students_df['GENDER'].apply(lambda g: g.value)
    student_count = students_df.shape[0]
    return students_df[['GENDER']].groupby(['GENDER']).size().sort_index(ascending=True)\
        .apply(lambda g: round_half_down(g / student_count))

# python3 general_query_10.py ../data/Student_Mobility.csv [out/pandas10-output]
if __name__ == "__main__":
    compute_task(gender_proportion, Student, argv[1:])

