#!/usr/bin/python3

"""General query 7

Student Mobility

Javier Fernández-Bravo Peñuela
Universitat Politècnica de València

Average age of European exchange students.

pandas - general_query_07.py
"""

from pandas_job.pandas_task import compute_task
from domain.student import Student
from util.functions import round_half_down

from sys import argv

def european_average_age(students_df):
    return students_df[['AGE']][lambda age: age > 0].mean().apply(round_half_down)

# python3 general_query_07.py ../data/Student_Mobility.csv [out/pandas07-output]
if __name__ == "__main__":
    compute_task(european_average_age, Student, argv[1:])
