#!/usr/bin/python3

"""General query 15

Student Mobility

Javier Fernández-Bravo Peñuela
Universitat Politécnica de València

Total exchange students having been taught per teaching language.

pandas - general_query_15.py
"""

from pandas_job.pandas_task import compute_task
from domain.student import Student

from sys import argv

def teaching_language(students_df):
    students_df['LANGUAGETAUGHT'] = students_df['LANGUAGETAUGHT']\
        .apply(lambda lt: lt.upper() if len(lt) > 0 else None)
    students_df = students_df.dropna(subset=['LANGUAGETAUGHT'])
    return students_df[['LANGUAGETAUGHT']].groupby(['LANGUAGETAUGHT']).size()

# python3 general_query_15.py ../data/Student_Mobility.csv [out/pandas15-output]
if __name__ == "__main__":
    compute_task(teaching_language, Student, argv[1:])
