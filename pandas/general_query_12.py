#!/usr/bin/python3

"""General query 12

Student Mobility

Javier Fernández-Bravo Peñuela
Universitat Politècnica de València

Gender proportion of exchange students received by Spanish host institutions.

pandas - general_query_12.py
"""

from pandas_job.pandas_task import compute_task
from domain.student import Student, MobilityType
from util.functions import round_half_down

from sys import argv

SPAIN = "ES"

def visitor_gender_proportion(students_df):
    students_df['HOST_COUNTRY'] = students_df[['MOBILITYTYPE','COUNTRYOFHOSTINSTITUTION','COUNTRYOFWORKPLACEMENT']]\
        .apply(host_country_udf, axis=1)
    students_df = students_df.dropna(subset=['HOST_COUNTRY', 'GENDER'])
    visitor_students_df = students_df[students_df['HOST_COUNTRY'] == SPAIN].copy()
    visitor_students_df.loc[:, 'GENDER'] = visitor_students_df['GENDER'].apply(lambda g: g.value)
    student_count = visitor_students_df.shape[0]
    return visitor_students_df[['GENDER']].groupby(['GENDER']).size().sort_index()\
        .apply(lambda g: round_half_down(g / student_count))

def host_country_udf(row):
    host_country = ''
    if row['MOBILITYTYPE'] in [MobilityType.STUDY, MobilityType.PLACEMENT]:
        if row['MOBILITYTYPE'] is MobilityType.STUDY:
            host_country = row['COUNTRYOFHOSTINSTITUTION']
        elif row['MOBILITYTYPE'] is MobilityType.PLACEMENT:
            host_country = row['COUNTRYOFWORKPLACEMENT']
    return host_country.rstrip() if host_country else None

# python3 general_query_12.py ../data/Student_Mobility.csv [out/pandas12-output]
if __name__ == "__main__":
    compute_task(visitor_gender_proportion, Student, argv[1:])
