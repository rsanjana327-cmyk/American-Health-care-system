
CREATE DATABASE IF NOT EXISTS HEALTHCARE_DB;
CREATE SCHEMA IF NOT EXISTS HEALTHCARE_DB.RAW;
CREATE OR REPLACE TABLE RAW.PATIENT_HEALTH_DATA (
    patient_id INTEGER,
    age INTEGER,
    gender STRING,
    state STRING,
    insurance_type STRING,
    hospital_visits INTEGER,
    annual_medical_cost_usd FLOAT,
    chronic_condition_flag INTEGER,
    admission_date DATE,
    age_group STRING,
    cost_bucket STRING,
    high_risk_patient STRING
);

CREATE OR REPLACE FILE FORMAT RAW.CSV_FILE_FORMAT
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1
NULL_IF = ('', 'NULL');


CREATE OR REPLACE STAGE RAW.HEALTHCARE_STAGE
FILE_FORMAT = RAW.CSV_FILE_FORMAT;

PUT file://american_health_system_data_100k.csv @RAW.HEALTHCARE_STAGE;


LIST @RAW.HEALTHCARE_STAGE;

COPY INTO RAW.PATIENT_HEALTH_DATA
FROM @RAW.HEALTHCARE_STAGE
FILE_FORMAT = RAW.CSV_FILE_FORMAT
ON_ERROR = 'CONTINUE';


-- Basic cleaning of data

CREATE OR REPLACE TABLE RAW.PATIENT_HEALTH_VALID 
LIKE RAW.PATIENT_HEALTH_DATA;


CREATE OR REPLACE TABLE RAW.PATIENT_HEALTH_REJECTS 
LIKE RAW.PATIENT_HEALTH_DATA;

ALTER TABLE RAW.PATIENT_HEALTH_REJECTS 
ADD COLUMN rejection_reason STRING;


INSERT INTO RAW.PATIENT_HEALTH_VALID
SELECT *
FROM RAW.PATIENT_HEALTH_DATA
WHERE
    patient_id IS NOT NULL
    AND age BETWEEN 0 AND 120
    AND annual_medical_cost_usd > 0
    AND insurance_type IN ('Medicare', 'Medicaid', 'Private', 'Uninsured');


    INSERT INTO RAW.PATIENT_HEALTH_REJECTS
SELECT
    *,
    CASE
        WHEN patient_id IS NULL THEN 'NULL patient_id'
        WHEN age NOT BETWEEN 0 AND 120 THEN 'Invalid age'
        WHEN annual_medical_cost_usd <= 0 THEN 'Invalid medical cost'
        WHEN insurance_type NOT IN ('Medicare', 'Medicaid', 'Private', 'Uninsured')
            THEN 'Invalid insurance type'
        ELSE 'Unknown error'
    END AS rejection_reason
FROM RAW.PATIENT_HEALTH_DATA
WHERE
    patient_id IS NULL
    OR age NOT BETWEEN 0 AND 120
    OR annual_medical_cost_usd <= 0
    OR insurance_type NOT IN ('Medicare', 'Medicaid', 'Private', 'Uninsured');

    SELECT COUNT(*) AS total_raw FROM RAW.PATIENT_HEALTH_DATA;

SELECT COUNT(*) AS valid_rows FROM RAW.PATIENT_HEALTH_VALID;

SELECT COUNT(*) AS rejected_rows FROM RAW.PATIENT_HEALTH_REJECTS;


SELECT rejection_reason, COUNT(*) 
FROM RAW.PATIENT_HEALTH_REJECTS
GROUP BY rejection_reason;

CREATE SCHEMA IF NOT EXISTS HEALTHCARE_DB.TRANSFORMED;

CREATE OR REPLACE TABLE TRANSFORMED.PATIENT_PROFILE AS
SELECT
    patient_id,

    age,
    CASE
        WHEN age BETWEEN 0 AND 17 THEN 'Child'
        WHEN age BETWEEN 18 AND 35 THEN 'Young Adult'
        WHEN age BETWEEN 36 AND 55 THEN 'Adult'
        WHEN age BETWEEN 56 AND 75 THEN 'Senior'
        ELSE 'Elderly'
    END AS age_group,

    -- Standardize gender
    UPPER(TRIM(gender)) AS gender,

    -- Standardize state codes
    UPPER(TRIM(state)) AS state,

    insurance_type,
    chronic_condition_flag

FROM RAW.PATIENT_HEALTH_VALID;


CREATE OR REPLACE TABLE TRANSFORMED.PATIENT_COST AS
SELECT
    patient_id,

    hospital_visits,
    annual_medical_cost_usd,

    CASE
        WHEN annual_medical_cost_usd <= 5000 THEN 'Low'
        WHEN annual_medical_cost_usd <= 15000 THEN 'Medium'
        WHEN annual_medical_cost_usd <= 30000 THEN 'High'
        ELSE 'Very High'
    END AS cost_bucket,

    -- High risk flag
    CASE
        WHEN chronic_condition_flag = 1
             AND annual_medical_cost_usd > 15000
        THEN 'Yes'
        ELSE 'No'
    END AS high_risk_patient,

    admission_date,
    YEAR(admission_date)  AS admission_year,
    MONTH(admission_date) AS admission_month

FROM RAW.PATIENT_HEALTH_VALID;


SELECT COUNT(*) FROM TRANSFORMED.PATIENT_PROFILE;
SELECT COUNT(*) FROM TRANSFORMED.PATIENT_COST;

SELECT age_group, COUNT(*) 
FROM TRANSFORMED.PATIENT_PROFILE
GROUP BY age_group;

SELECT cost_bucket, COUNT(*) 
FROM TRANSFORMED.PATIENT_COST
GROUP BY cost_bucket;

CREATE SCHEMA IF NOT EXISTS HEALTHCARE_DB.ANALYTICS;


CREATE OR REPLACE TABLE ANALYTICS.DIM_PATIENT AS
SELECT DISTINCT
    patient_id           AS patient_key,
    age,
    age_group,
    gender,
    chronic_condition_flag
FROM TRANSFORMED.PATIENT_PROFILE;


CREATE OR REPLACE TABLE ANALYTICS.DIM_LOCATION AS
SELECT DISTINCT
    state AS state_code
FROM TRANSFORMED.PATIENT_PROFILE;

CREATE OR REPLACE TABLE ANALYTICS.DIM_INSURANCE AS
SELECT DISTINCT
    insurance_type
FROM TRANSFORMED.PATIENT_PROFILE;



CREATE OR REPLACE TABLE ANALYTICS.DIM_DATE AS
SELECT DISTINCT
    admission_date        AS date_key,
    admission_year,
    admission_month
FROM TRANSFORMED.PATIENT_COST;


CREATE OR REPLACE TABLE ANALYTICS.FACT_HEALTHCARE_COST AS
SELECT
    c.patient_id            AS patient_key,
    d.date_key,
    p.age_group,
    l.state_code,
    i.insurance_type,

    c.annual_medical_cost_usd   AS total_cost,
    c.hospital_visits,
    
    CASE 
        WHEN c.high_risk_patient = 'Yes' THEN 1 
        ELSE 0 
    END AS high_risk_patient_count

FROM TRANSFORMED.PATIENT_COST c
JOIN TRANSFORMED.PATIENT_PROFILE p
    ON c.patient_id = p.patient_id
JOIN ANALYTICS.DIM_LOCATION l
    ON p.state = l.state_code
JOIN ANALYTICS.DIM_INSURANCE i
    ON p.insurance_type = i.insurance_type
JOIN ANALYTICS.DIM_DATE d
    ON c.admission_date = d.date_key;



---- 1️⃣ Which state has the highest healthcare cost per patient?

--Logic:
---Total cost ÷ distinct patients per state
SELECT
    state_code,
    SUM(total_cost) / COUNT(DISTINCT patient_key) AS avg_cost_per_patient
FROM ANALYTICS.FACT_HEALTHCARE_COST
GROUP BY state_code
ORDER BY avg_cost_per_patient DESC;

---- 2️⃣ Cost comparison across insurance types

SELECT
    insurance_type,
    SUM(total_cost) AS total_healthcare_cost,
    AVG(total_cost) AS avg_cost_per_record
FROM ANALYTICS.FACT_HEALTHCARE_COST
GROUP BY insurance_type
ORDER BY total_healthcare_cost DESC;
--- 3️⃣ % of high-risk patients by age group

SELECT
    age_group,
    ROUND(
        SUM(high_risk_patient_count) 
        / COUNT(DISTINCT patient_key) * 100,
        2
    ) AS high_risk_percentage
FROM ANALYTICS.FACT_HEALTHCARE_COST
GROUP BY age_group
ORDER BY high_risk_percentage DESC;


--- 4️⃣ Monthly admission trends
SELECT
    admission_year,
    admission_month,
    COUNT(*) AS total_admissions
FROM HEALTHCARE_DB.ANALYTICS.DIM_DATE
GROUP BY admission_year, admission_month
ORDER BY admission_year, admission_month;


--- 4️⃣ Monthly admission trends


SELECT
    CASE 
        WHEN chronic_condition_flag = 1 THEN 'Chronic'
        ELSE 'Non-Chronic'
    END AS patient_type,
    SUM(total_cost) AS total_cost,
    ROUND(AVG(total_cost), 2) AS avg_cost
FROM ANALYTICS.FACT_HEALTHCARE_COST f
JOIN ANALYTICS.DIM_PATIENT p
    ON f.patient_key = p.patient_key
GROUP BY patient_type;



CREATE OR REPLACE VIEW ANALYTICS.VW_HEALTHCARE_ANALYTICS AS
SELECT
    -- Patient Dimension
    p.patient_key,
    p.age,
    p.age_group,
    p.gender,
    p.chronic_condition_flag,

    -- Location Dimension
    l.state_code,

    -- Insurance Dimension
    i.insurance_type,

    -- Date Dimension
    d.date_key           AS admission_date,
    d.admission_year,
    d.admission_month,

    -- Fact Measures
    f.total_cost,
    f.hospital_visits,
    f.high_risk_patient_count

FROM ANALYTICS.FACT_HEALTHCARE_COST f

JOIN ANALYTICS.DIM_PATIENT p
    ON f.patient_key = p.patient_key

JOIN ANALYTICS.DIM_LOCATION l
    ON f.state_code = l.state_code

JOIN ANALYTICS.DIM_INSURANCE i
    ON f.insurance_type = i.insurance_type

JOIN ANALYTICS.DIM_DATE d
    ON f.date_key = d.date_key;



select * from HEALTHCARE_DB.ANALYTICS.VW_HEALTHCARE_ANALYTICS;















