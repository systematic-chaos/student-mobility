# Student Mobility

Big Data Analytics technologies can manage large data collections, retrieving useful knowledge from them that data analysts can direct to reach conclusions. This work can be optimally performed on a single node if the whole dataset fits in the machine's main memory and the structure of data is suitable for the utilisation of traditional database management systems. However, these limitations in storage and processing usually create the need of finding another way to perform data analysis that bypasses such restrictions. The adoption of distributed programming paradigms, such as MapReduce, enable the processing of a huge volume of data on a cluster of nodes that run in parallel, leveraging the advantages provided by distributed computing, which become even more prominent in case of turning to the infrastructure of a cloud provider.

Three different data processing technologies are used at this project:

* **Apache Hadoop** is a framework that allows for the distributed processing of large data sets across clusters of computers using simple programming models. It is designed to scale up from single servers to thousands of machines, each offering local computation and storage. Rather than rely on hardware to deliver high-availability, the library itself is designed to detect and handle failures at the application layer, so delivering a highly-available service on top of a cluster of computers, each of which may be prone to failures.
* **Apache Spark** is a multi-language engine for executing data engineering, data science, and machine learning on single-node machines or clusters.
  * Unifies the processing of data in batches and real-time streaming.
  * Executes fast, distributed ANSI SQL queries for dashboarding and ad-hoc reporting. Runs faster than most data warehouses.
  * Performs Exploratory Data Analysis (EDA) on petabyte-scale data without having to resort to down-sampling.
  * Trains maching learning algorithms, scaling from a laptop to fault-tolerant clusters of thousands of machines, all while using the same code.
* **pandas** is a fast, powerful, flexible and easy to use open-source data analysis and manipulation tool, built on top of the Python programming language.


A 93MB-sized data repository, comprising publicly available information about Erasmus students, is analyzed. It is structured in a tabular format, featuring the next columns along with their data types:

| Field name | Data type |
| ---------- | --------- |
| home_institution            | `string`  |
| country_of_home_institution | `string`  |
| age                         | `integer` |
| gender                      | `enum` (female: 'F', male: 'M') |
| study_level                 | `enum` (first: '1', second: '2', superior: 'S') |
| mobility_type               | `enum` (study: 'S', placement: 'P', other: 'C') |
| host_institution            | `string`  |
| country_of_host_institution | `string`  |
| country_of_work_placement   | `string`  |
| length_study_period         | `float`   |
| length_work_placement       | `float`   |
| study_start_date            | `date`    |
| placement_start_date        | `date`    |
| taught_host_lang            | `boolean` (`true`: 'Y', `false`: 'N') |
| language_taught             | `string`  |
| study_grant                 | `float`   |
| placement_grant             | `float`   |

A series of questions are set out and subsequently answered by implementing queries on the data collection by each of these three technologies. This approach displays how to make use of each of these technologies, resorting to their respective particular mechanisms, to solve the same problem. This leads to establishing a comparison of different technologies in terms of the power and expressivity of their APIs when coding on them, their throughput and the complexity of their setup and deployment. In addition, relying on three different and independent implementations of the same suite of problems contributes as a correctness guarantee of their output, since all results can be checked to be identical. In order to also compare how different technologies perform, a much larger dataset could be used (in the order of hundreds of gigabytes), where a centralized solution could not operate and distributed solutions would certainly make a difference.

The following questions are formulated and implemented as queries:

_General queries:_

1. Total exchange students sent by each country.
2. Total exchange students received by each country.
3. Proportion of exchange students sent/received per country.
4. Total exchange students sent by each Spanish institution.
5. Total exchange students received by each Spanish institution.
6. Destination country preferred by each country's students.
7. Average age of European exchange students.
8. Average age of Spanish exchange students.
9. Average age of exchange students received by Spanish host institutions.
10. Gender proportion of European exchange students.
11. Gender proportion of Spanish exchange students.
12. Gender proportion of exchange students received by Spanish host institutions.
13. Total of exchange students that stay abroad for the fall semester, the spring semester, and the whole academic year, respectively.
14. Proportion, of those students that initially stay abroad just for the fall semester, that extend their exchange period to the next spring semester.
15. Total exchange students having been taught per teaching language.
16. Teaching languages used and proportion of students that, having been taught in that language, received specific training in that language's utilization. This query has been restricted to the English language, for the sake of simplicity in the interpretation of results and data consistency.
17. Average economic grant per European exchange student.
18. Average economic grant per Spanish exchange student.
19. Distribution of exchange students in terms of their occupation (study or internship/placement) and level (graduate or postgraduate). Therefore, four groups are considered: graduate students, graduate interns, postgraduate students and postgraduate interns.

_Specific queries:_

1. How many students, from the same home country as the 22-aged individual, whose home university code is "CIUDA-R" and whose host university code is "S VASTERA", took their Erasmus exchange in the same host university.
2. How many students, from the home university that the campus with code "CIUDA" belongs to, took their Erasmus exchange in the host university that the campus with code "VASTERA" belongs to. A reference student is provided in order to correlate those campuses to their corresponding universities.
