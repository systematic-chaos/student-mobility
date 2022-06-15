#!/usr/bin/python3

"""General query 3

Student Mobility

Javier Fernández-Bravo Peñuela
Universitat Politècnica de València

Proportion of exchange students sent/received per country.

pandas - general_query_03.py
"""

from pandas_job.pandas_task import compute_task
from domain.student import Student

from sys import argv

def student_delivery_reception_proportion(students_df):
    delivery_df  = students_df[['COUNTRYOFHOMEINSTITUTION']].groupby(['COUNTRYOFHOMEINSTITUTION']).size()
    reception_df = students_df[['COUNTRYOFHOSTINSTITUTION']].groupby(['COUNTRYOFHOSTINSTITUTION']).size().filter(regex="[A-Z]+")
    return delivery_df.divide(reception_df, fill_value=0)

# python3 general_query_03.py ../data/Student_Mobility.csv [out/pandas03-output]
if __name__ == "__main__":
    compute_task(student_delivery_reception_proportion, Student, argv[1:])
