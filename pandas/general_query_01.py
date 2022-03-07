#!/usr/bin/python3

"""General query 1

Student Mobility

Javier Fernández-Bravo Peñuela
Universitat Politècnica de València

Total exchange students sent by each country.

pandas - general_query_01.py
"""

from pandas_job.pandas_task import compute_task
from domain.student import Student

from sys import argv

def students_send_by_home_country(students_df):
    return students_df[['COUNTRYOFHOMEINSTITUTION']].groupby(['COUNTRYOFHOMEINSTITUTION']).size()

# python3 general_query_01.py ../data/Student_Mobility.csv [out/pandas01-output]
if __name__ == "__main__":
    compute_task(students_send_by_home_country, Student, argv[1:])
