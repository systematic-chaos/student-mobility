#!/usr/bin/python3

"""General query 9

Student Mobility

Javier Fernández-Bravo Peñuela
Universitat Politècnica de València

Average age of exchange students received by Spanish host institutions.

pandas - general_query_09.py
"""

from pandas_job.pandas_task import compute_task
from domain.student import Student, MobilityType
from util.functions import round_half_down

from sys import argv

SPAIN = "ES"

def guiri_average_age(students_df):
    students_df['HOST_COUNTRY'] = students_df[['MOBILITYTYPE','COUNTRYOFHOSTINSTITUTION','COUNTRYOFWORKPLACEMENT']]\
        .apply(host_country_udf, axis=1)
    students_df = students_df.dropna(subset=['HOST_COUNTRY', 'AGE'])
    guiri_students_df = students_df[(students_df['HOST_COUNTRY'] == SPAIN) & (students_df['AGE'] > 0)]
    return guiri_students_df[['AGE']].mean().apply(round_half_down)


def host_country_udf(row):
    host_country = ''
    if row['MOBILITYTYPE'] in [MobilityType.STUDY, MobilityType.PLACEMENT]:
        if row['MOBILITYTYPE'] is MobilityType.STUDY:
            host_country = row['COUNTRYOFHOSTINSTITUTION']
        elif row['MOBILITYTYPE'] is MobilityType.PLACEMENT:
            host_country = row['COUNTRYOFWORKPLACEMENT']
    return host_country.rstrip() if host_country else None

# python3 general_query_09.py ../data/Student_Mobility.csv [out/pandas09-output]
if __name__ == "__main__":
    compute_task(guiri_average_age, Student, argv[1:])
