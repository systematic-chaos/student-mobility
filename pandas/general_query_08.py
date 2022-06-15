#!/usr/bin/python3

"""General query 8

Student Mobility

Javier Fernández-Bravo Peñuela
Universitat Politècnica de València

Average age of Spanish exchange students.

pandas - general_query_08.py
"""

from pandas_job.pandas_task import compute_task
from domain.student import Student
from util.functions import round_half_down

from sys import argv

SPAIN = "ES"

def spanish_average_age(students_df):
    spanish_students_df = students_df[(students_df['COUNTRYOFHOMEINSTITUTION'] == SPAIN) & (students_df['AGE'] > 0)]
    age = spanish_students_df[['AGE']].mean().apply(round_half_down)
    return age

# python3 general_query_08.py ../data/Student_Mobility.csv [out/pandas08-output]
if __name__ == "__main__":
    compute_task(spanish_average_age, Student, argv[1:])
