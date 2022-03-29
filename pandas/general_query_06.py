#!/usr/bin/python3

"""General query 6

Student Mobility

Javier Fernández-Bravo Peñuela
Universitat Politècnica de València

Destination country preferred by each country's students.

pandas - general_query_06.py
"""

from pandas_job.pandas_task import compute_task
from domain.student import Student, MobilityType

from sys import argv

def students_favorite_destination_country(students_df):
    students_df['HOST_COUNTRY'] = students_df[['MOBILITYTYPE','COUNTRYOFHOSTINSTITUTION','COUNTRYOFWORKPLACEMENT']]\
        .apply(host_country_udf, axis=1)
    students_df = students_df.dropna(subset=['HOST_COUNTRY'])
    students_df = students_df.groupby(['COUNTRYOFHOMEINSTITUTION', 'HOST_COUNTRY'], as_index=False, sort=False).size()
    students_df = students_df[students_df['size'] == students_df.groupby(['COUNTRYOFHOMEINSTITUTION'])['size'].transform('max')]
    students_favorite_destination_country_df = students_df.sort_values('COUNTRYOFHOMEINSTITUTION')
    return students_favorite_destination_country_df

def host_country_udf(row):
    host_country = ''
    if row['MOBILITYTYPE'] in [MobilityType.STUDY, MobilityType.PLACEMENT]:
        if row['MOBILITYTYPE'] is MobilityType.STUDY:
            host_country = row['COUNTRYOFHOSTINSTITUTION']
        elif row['MOBILITYTYPE'] is MobilityType.PLACEMENT:
            host_country = row['COUNTRYOFWORKPLACEMENT']
    return host_country.rstrip() if host_country else None

# python3 general_query_06.py ../data/Student_Mobility.csv [out/pandas06-output]
if __name__ == "__main__":
    compute_task(students_favorite_destination_country, Student, argv[1:])