#!/usr/bin/python3

"""General query 2

Student Mobility

Javier Fernández-Bravo Peñuela
Universitat Politècnica de València

Total exchange students received by each country.

pandas - general_query_02.py
"""

from pandas_job.pandas_task import compute_task
from domain.student import Student

from sys import argv

def students_received_by_host_country(students_df):
    return students_df[['COUNTRYOFHOSTINSTITUTION']].groupby(['COUNTRYOFHOSTINSTITUTION']).size()

# python3 general_query_02.py ../data/Student_Mobility.csv [out/pandas02-output]
if __name__ == "__main__":
    compute_task(students_received_by_host_country, Student, argv[1:])
