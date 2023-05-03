import re

def nullable_dollars(value):
    if value is None or str(value) in ['NaN', 'nan']:
        return -1
    if not isinstance(value, str):
        raise Exception(f'dollars should be None or string, provided value = {value}')
    if not value.startswith("$"):
        raise Exception(f'dollars should start with $, provided value = {value}')
    return float(value.replace("$", "").replace(",", ""))

def boolean(value):
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, str):
        value = value.lower()
    if value in ['yes', 'y', 1]:
        return True
    if value in ['no', 'n', 0]:
        return False
    raise Exception(f'not a boolean, provided value = {value}')

def nullable_str(value):
    if value is None or str(value) in ['NaN', 'nan']:
        return "__MISSING_DATA__"
    value = value.lower()
    if value.startswith("z_"):
        value = value[2:]
    return re.sub(r"[^a-zA-Z0-9]+", ' ',  value)

def sex(value):
    if not isinstance(value, str):
        raise Exception(f'sex should be a string, provided value = {value}')
    if value in ['f', 'female']:
        return 'f'
    if value in ['m', 'male']:
        return 'm'
    raise Exception(f'not a sex, provided value = {value}')

def commercial_car_use(value):
    if not isinstance(value, str):
        raise Exception(f'car use should be a string, provided value = {value}')
    if value == 'commercial':
        return True
    if value == 'private':
        return False
    raise Exception(f'not a car use, provided value = {value}')

def urban_city(value):
    if not isinstance(value, str):
        raise Exception(f'urban city should be a string, provided value = {value}')
    if value.startswith('highly urban'):
        return True
    if value.startswith('highly rural'):
        return False
    raise Exception(f'not a urban city boolean, provided value = {value}')

def education_level(value):
    order = ['<High School', 'z_High School', 'Bachelors', 'Masters', 'PhD']
    return order.index(value)

def nullable_int(value):
    if value is None or str(value) in ['NaN', 'nan']:
        return -1
    return int(value)


MAPPING_NAMES = {
    'INDEX': 'id',
    'TARGET_FLAG': 'label',
    'TARGET_AMT': 'amt',
    'KIDSDRIV': 'kids_driv',
    'AGE': 'age',
    'HOMEKIDS': 'home_kids',
    'YOJ': 'yoj',
    'INCOME': 'income',
    'PARENT1': 'parent1',
    'HOME_VAL': 'home_val',
    'MSTATUS': 'm_status',
    'SEX': 'sex',
    'EDUCATION': 'education_level',
    'JOB': 'job',
    'TRAVTIME': 'trav_time',
    'CAR_USE': 'commercial_car_use',
    'BLUEBOOK': 'blue_book',
    'TIF': 'tif',
    'CAR_TYPE': 'car_type',
    'RED_CAR': 'red_car',
    'OLDCLAIM': 'old_claim',
    'CLM_FREQ': 'clm_freq',
    'REVOKED': 'revoced',
    'MVR_PTS': 'mvr_pts',
    'CAR_AGE': 'car_age',
    'URBANICITY': 'urban_city',
}

TRANSFORM_FUNCTIONS = {
    'id': int,
    'label': boolean,
    'amt': float,
    'kids_driv': int,
    'age': nullable_int,
    'home_kids': int,
    'yoj': nullable_int,
    'income': nullable_dollars,
    'parent1': boolean,
    'home_val': nullable_dollars,
    'm_status': lambda v: boolean(nullable_str(v)),
    'sex': lambda v: sex(nullable_str(v)),
    'education_level': education_level,
    'job': nullable_str,
    'trav_time': int,
    'commercial_car_use': lambda v: commercial_car_use(nullable_str(v)),
    'blue_book': nullable_dollars,
    'tif': int,
    'car_type': nullable_str,
    'red_car': boolean,
    'old_claim': nullable_dollars,
    'clm_freq': int,
    'revoced': boolean,
    'mvr_pts': int,
    'car_age': nullable_int,
    'urban_city': lambda v: urban_city(nullable_str(v)),
}

