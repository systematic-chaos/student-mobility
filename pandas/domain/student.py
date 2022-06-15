"""pandas - domain/student.py

Student Mobility

Javier Fernández-Bravo Peñuela
Universitat Politècnica de València
"""

import locale
from datetime import date, datetime
from enum import Enum

#locale.setlocale(locale.LC_NUMERIC, "en_us")
#locale.setlocale(locale.LC_TIME, "en_us")

class Student:
    def __init__(self, home_institution, country_of_home_institution, age,
                gender, study_level, mobility_type, host_institution, country_of_host_institution,
                country_of_work_placement, length_study_period=None, length_work_placement=None,
                study_start_date=None, placement_start_date=None, taught_host_lang=None,
                language_taught=None, study_grant=None, placement_grant=None):
        self.set_home_institution(home_institution)
        self.set_country_of_home_institution(country_of_home_institution)
        self.set_age(age)
        self.set_gender(gender)
        self.set_study_level(study_level)
        self.set_mobility_type(mobility_type)
        self.set_host_institution(host_institution)
        self.set_country_of_host_institution(country_of_host_institution)
        self.set_country_of_work_placement(country_of_work_placement)

        self.set_length_study_period(length_study_period)
        self.set_length_work_placement(length_work_placement)
        self.set_study_start_date(study_start_date)
        self.set_placement_start_date(placement_start_date)
        self.set_taught_host_lang(taught_host_lang)
        self.set_language_taught(language_taught)
        self.set_study_grant(study_grant)
        self.set_placement_grant(placement_grant)
    
    def get_home_institution(self):
        return self.home_institution
    
    def set_home_institution(self, home_institution):
        self.home_institution = string_factory(home_institution)
    
    def get_country_of_home_institution(self):
        return self.country_of_home_institution
    
    def set_country_of_home_institution(self, country_of_home_institution):
        self.country_of_home_institution = string_factory(country_of_home_institution)
    
    def get_age(self):
        return self.age
    
    def set_age(self, age):
        self.age = integer_factory(age)
    
    def get_gender(self):
        return self.gender
    
    def set_gender(self, gender):
        self.gender = gender_factory(gender)
    
    def get_study_level(self):
        return self.study_level
    
    def set_study_level(self, study_level):
        self.study_level = study_level_factory(study_level)
    
    def get_mobility_type(self):
        return self.mobility_type
    
    def set_mobility_type(self, mobility_type):
        self.mobility_type = mobility_type_factory(mobility_type)
    
    def get_host_institution(self, host_institution):
        return self.host_institution

    def set_host_institution(self, host_institution):
        self.host_institution = string_factory(host_institution)
    
    def get_country_of_host_institution(self):
        return self.country_of_host_institution
    
    def set_country_of_host_institution(self, country_of_host_institution):
        self.country_of_host_institution = string_factory(country_of_host_institution)
    
    def get_country_of_work_placement(self):
        return self.country_of_work_placement
    
    def set_country_of_work_placement(self, country_of_work_placement):
        self.country_of_work_placement = string_factory(country_of_work_placement)
    
    def get_length_study_period(self):
        return self.length_study_period
    
    def set_length_study_period(self, length_study_period):
        self.length_study_period = float_factory(length_study_period)
    
    def get_length_work_placement(self):
        return self.length_work_placement
    
    def set_length_work_placement(self, length_work_placement):
        self.length_work_placement = float_factory(length_work_placement)
    
    def get_study_start_date(self):
        return self.study_start_date
    
    def set_study_start_date(self, study_start_date):
        self.study_start_date = date_factory(study_start_date)
    
    def get_placement_start_date(self):
        return self.study_start_date
    
    def set_placement_start_date(self, placement_start_date):
        self.placement_start_date = date_factory(placement_start_date)
    
    def get_taught_host_lang(self):
        return self.taught_host_lang
    
    def set_taught_host_lang(self, taught_host_lang):
        self.taught_host_lang = boolean_factory(taught_host_lang)
    
    def get_language_taught(self):
        return self.language_taught
    
    def set_language_taught(self, language_taught):
        self.language_taught = string_factory(language_taught)
    
    def get_study_grant(self):
        return self.study_grant
    
    def set_study_grant(self, study_grant):
        self.study_grant = float_factory(study_grant)
    
    def get_placement_grant(self):
        return self.placement_grant
    
    def set_placement_grant(self, placement_grant):
        self.placement_grant = float_factory(placement_grant)
    
    def to_string(self):
        def default_string(string): return string if string is not None else ''
        def default_integer(integer): return integer if integer is not None else 0.
        def default_date(date_value): return date_value.isoformat() if date_value is not None else ''
        def default_boolean(boolean): return boolean if boolean is not None else False

        return ';'.join([
            self.home_institution,
            self.country_of_home_institution,
            self.age,
            self.gender,
            self.study_level,
            self.mobility_type,
            self.host_institution,
            default_string(self.country_of_host_institution),
            default_string(self.country_of_work_placement),
            default_integer(self.length_study_period),
            default_integer(self.length_work_placement),
            default_date(self.study_start_date),
            default_date(self.placement_start_date),
            default_boolean(self.taught_host_lang),
            default_string(self.language_taught),
            default_integer(self.study_grant),
            default_integer(self.placement_grant)
        ])
    
    @staticmethod
    def from_string(stringified_student):
        num_attributes_basic = 9
        num_attributes_full = 17
        student_fields = stringified_student.split(';')
        if len(student_fields) < num_attributes_basic or len(student_fields) > num_attributes_full:
            return None
        
        converters = Student.get_converters()

        (home_institution,
        country_of_home_institution,
        age,
        gender,
        study_level,
        mobility_type,
        host_institution,
        country_of_host_institution,
        country_of_work_placement) = [converters[n](student_fields[n]) for n in range(num_attributes_basic)]

        if len(student_fields) < num_attributes_full:
            student = Student(home_institution, country_of_home_institution, age, gender,
                            study_level, mobility_type, host_institution,
                            country_of_host_institution, country_of_work_placement)
        else:
            (length_study_period,
            length_work_placement,
            study_start_date,
            placement_start_date,
            taught_host_lang,
            language_taught,
            study_grant,
            placement_grant) = [converters[n](student_fields[n]) for n in range(num_attributes_basic, num_attributes_full)]
            student = Student(home_institution, country_of_home_institution, age, gender,
                            study_level, mobility_type, host_institution,
                            country_of_host_institution, country_of_work_placement,
                            length_study_period, length_work_placement,
                            study_start_date, placement_start_date, taught_host_lang,
                            language_taught, study_grant, placement_grant)
        return student
    
    @staticmethod
    def get_converters():
        return [
            string_factory,         # 0:  home_institution
            string_factory,         # 1:  country_of_home_institution
            integer_factory,        # 2:  age
            gender_factory,         # 3:  gender
            study_level_factory,    # 4:  study_level
            mobility_type_factory,  # 5:  mobility_type
            string_factory,         # 6:  host_institution
            string_factory,         # 7:  country_of_host_institution
            string_factory,         # 8:  country_of_work_placement
            float_factory,          # 9:  length_study_period
            float_factory,          # 10: length_work_placement
            date_factory,           # 11: study_start_date
            date_factory,           # 12: placement_start_date
            boolean_factory,        # 13: taught_host_lang
            string_factory,         # 14: language_taught
            float_factory,          # 15: study_grant
            float_factory           # 16: placement_grant
        ]

class Gender(Enum):
    MALE = 'M'
    FEMALE = 'F'

class StudyLevel(Enum):
    FIRST = '1'
    SECOND = '2'
    SUPERIOR = 'S'

class MobilityType(Enum):
    STUDY = 'S'
    PLACEMENT = 'P'
    OTHER = 'C'

def string_factory(string):
    return str(string) if not isNone(string) else ''

def integer_factory(integer):
    return int(integer) if not isNone(integer) else 0

def float_factory(float_number):
    return parse_float(float_number) if not isNone(float_number) else 0.

def boolean_factory(boolean):
    return bool_from_char(boolean)

def date_factory(date_string):
    return date_from_string(date_string)

def gender_factory(gender):
    return gender_from_value(gender)

def study_level_factory(study_level):
    return study_level_from_value(study_level)

def mobility_type_factory(mobility_type):
    return mobility_type_from_value(mobility_type)

def bool_from_char(input):
    YES = 'Y'
    NO = 'N'
    
    output = False
    if isinstance(input, str) and len(input) > 0:
        if input[0] == YES:
            output = True
        elif input[0] == NO:
            output = False
    return output

def date_from_string(input):
    if isinstance(input, date):
        return input
    elif isNone(input):
        return None
    
    split = '/' if '/' in input else '-'
    pattern = None
    output = None
    if len(input) == 7:
        if not input.endswith("0000"):
            input = '01' + split + input
            pattern = '%d' + split + '%m' + split + '%Y'
    elif len(input) == 20:
        pattern = '%d' + split + '%b' + split + '%Y %H.%M.%S'
    
    if pattern is not None:
        output = datetime.strptime(input.lower(), pattern).date()
    return output

def parse_float(input):
    if isinstance(input, str):
        input = input.replace(',', '.')
    output = float(input)
    return output

def gender_from_value(gender_value):
    gender_output = None
    if isinstance(gender_value, Gender):
        gender_output = Gender
    elif isinstance(gender_value, str) and len(gender_value) == 1:
        if gender_value == Gender.MALE.value:
            gender_output = Gender.MALE
        elif gender_value == Gender.FEMALE.value:
            gender_output = Gender.FEMALE
    return gender_output

def study_level_from_value(study_level_value):
    study_level_output = None
    if isinstance(study_level_value, StudyLevel):
        study_level_output = study_level_value
    elif not isNone(study_level_value):
        study_level_value = str(study_level_value)
        if study_level_value == StudyLevel.FIRST.value:
            study_level_output = StudyLevel.FIRST
        elif study_level_value == StudyLevel.SECOND.value:
            study_level_output = StudyLevel.SECOND
        elif study_level_value == StudyLevel.SUPERIOR.value:
            study_level_output = StudyLevel.SUPERIOR
    return study_level_output

def mobility_type_from_value(mobility_type_value):
    mobility_type_output = None
    if isinstance(mobility_type_value, MobilityType):
        mobility_type_output = mobility_type_value
    elif not isNone(mobility_type_value):
        mobility_type_value = str(mobility_type_value)
        if mobility_type_value == MobilityType.STUDY.value:
            mobility_type_output = MobilityType.STUDY
        elif mobility_type_value == MobilityType.PLACEMENT.value:
            mobility_type_output = MobilityType.PLACEMENT
        elif mobility_type_value == MobilityType.OTHER.value:
            mobility_type_output = MobilityType.OTHER
    return mobility_type_output

def isNone(arg):
    return arg is None or arg == ''
