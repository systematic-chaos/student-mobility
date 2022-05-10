#!/usr/bin/python3

"""General query 16

Student Mobility

Javier Fernández-Bravo Peñuela
Universitat Politècnica de València

Teaching languages used and proportion of students that, having been taught
in that language, received specific training in that language's utilization.
This query has been restricted to the English language, for the sake of
simplicity in the interpretation of results and data consistency.

pandas - general_query_16.py
"""

from pandas_job.pandas_task import compute_task
from domain.student import Student
from util.functions import round_half_down

import pandas as pd
from sys import argv

ENGLISH = "EN"

def foreign_language_training_proportion(students_df):
    students_df['LANGUAGETAUGHT'] = students_df['LANGUAGETAUGHT']\
        .apply(lambda lt: lt.upper() if len(lt) > 0 else None)
    students_df['TAUGHTHOSTLANG'] = students_df['TAUGHTHOSTLANG']\
        .apply(lambda thl: True if thl else False)
    students_df = students_df[students_df['LANGUAGETAUGHT'].eq(ENGLISH)]
    language_teaching = students_df.groupby(['LANGUAGETAUGHT', 'TAUGHTHOSTLANG']).size()

    taught_languages = { index[0] for index, _ in language_teaching.iteritems() }
    return pd.Series(compute_language_teaching_proportion(language_teaching, taught_languages))

def compute_language_teaching_proportion(language_teaching, taught_languages):
    language_teaching_proportion = {}
    for lang in sorted(taught_languages):
        taught_yes = language_teaching[(lang, True)] if (lang, True) in language_teaching else 0
        taught_no  = language_teaching[(lang, False)] if (lang, False) in language_teaching else 0
        language_teaching_proportion[lang] = round_half_down(taught_yes / (taught_yes + taught_no))
    return language_teaching_proportion

# python3 general_query_16.py ../data/Student_Mobility.csv [out/pandas16-output]
if __name__ == "__main__":
    compute_task(foreign_language_training_proportion, Student, argv[1:])
