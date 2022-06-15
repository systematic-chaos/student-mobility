#!/usr/bin/python3

"""General query 19

Student Mobility

Javier Fernández-Bravo Peñuela
Universitat Politècnica de València

Distribution of exchange students in terms of their occupation
(study or internship/placement) and level (graduate or postgraduate).
Therefore, four groups are considered: graduate students, graduate interns,
postgraduate students and postgraduate interns.

pandas - general_query_19.py
"""

from pandas_job.pandas_task import compute_task
from domain.student import Student
from util.functions import round_half_down

from sys import argv

def mobility_type_study_level_distribution(students_df):
    students_df = students_df.dropna(subset=['MOBILITYTYPE', 'LEVELSTUDY'])
    mtls_groups_df = students_df.groupby(['MOBILITYTYPE', 'LEVELSTUDY'], as_index=False, sort=False).size()
    mtls_groups_df['MOBILITYTYPE'] = mtls_groups_df['MOBILITYTYPE'].transform(lambda mt: mt.value)
    mtls_groups_df['STUDYLEVEL'] = mtls_groups_df['LEVELSTUDY'].apply(lambda sl: sl.value)

    num_students = mtls_groups_df['size'].sum()
    mtls_groups_df['proportion'] = mtls_groups_df['size'].apply(lambda mtls: round_half_down(mtls / num_students))

    mtls_groups_df = mtls_groups_df[['MOBILITYTYPE', 'STUDYLEVEL', 'proportion']]
    mtls_groups_df = mtls_groups_df.set_index(['MOBILITYTYPE', 'STUDYLEVEL']).sort_index().squeeze()
    mtls_groups_df.index = mtls_groups_df.index.map(lambda mtls: '{}\t{}'.format(*mtls))
    return mtls_groups_df

# python3 general_query_19.py ../data/Student_Mobility.csv [out/pandas19-output]
if __name__ == "__main__":
    compute_task(mobility_type_study_level_distribution, Student, argv[1:])
