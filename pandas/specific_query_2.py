#!/usr/bin/python3

"""Specific query 2

Student Mobility

Javier Fernández-Bravo Peñuela
Universitat Politècnica de València

How many students, from the university that the campus with code "CIUDA" belongs to,
took their Erasmus exchange in the host university that the campus with code "VASTERA"
belongs to. A reference student is provided in order to correlate those campuses to
their corresponding universities.

pandas - specific_query_2.py
"""

from pandas_job.pandas_task import compute_task
from domain.student import Student

import pandas as pd
from sys import argv

HOME_UNIVERSITY = "CIUDA-R"
HOST_UNIVERSITY = "VASTERA"
AGE = 22

def university_companions(students_df):
    students_df['HOMEINSTITUTION'] = students_df['HOMEINSTITUTION'].apply(uni_sub_str)
    students_df['HOSTINSTITUTION'] = students_df['HOSTINSTITUTION'].apply(uni_sub_str)

    me = students_df.iloc[\
        students_df.index[(students_df['HOMEINSTITUTION'].str.startswith(HOME_UNIVERSITY))\
            & (students_df['HOSTINSTITUTION'].str.startswith(HOST_UNIVERSITY))\
                & (students_df['AGE'].eq(AGE))][0]]
    
    companions = students_df[(students_df['HOMEINSTITUTION'] == me['HOMEINSTITUTION'])\
        & (students_df['HOSTINSTITUTION'] == me['HOSTINSTITUTION'])]
    return pd.Series({ 'UNIVERSITY_COMPANIONS': companions.shape[0] })

def uni_sub_str(uni):
    return uni[3:10] if len(uni) >= 10 else uni

# python3 specific_query_2.py ../data/Student_Mobility.csv [out/pandas-2-output]
if __name__ == "__main__":
    compute_task(university_companions, Student, argv[1:])
