CREATE DATABASE mlflow;
CREATE DATABASE airflow;
CREATE DATABASE feast;

\c feast;

CREATE TABLE IF NOT EXISTS drivers (
    id integer,
    label boolean,
    amt real,
    kids_driv integer,
    age integer,
    home_kids integer,
    yoj integer,
    income real,
    parent1 boolean,
    home_val real,
    m_status boolean,
    sex text,
    education_level integer,
    job text,
    trav_time integer,
    commercial_car_use boolean,
    blue_book real,
    tif integer,
    car_type text,
    red_car boolean,
    old_claim real,
    clm_freq integer,
    revoced boolean,
    mvr_pts integer,
    car_age integer,
    urban_city boolean,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY(id, created_at)
);
