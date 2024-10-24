WITH adult_patients AS (
    SELECT subject_id
    FROM physionet-data.mimiciv_hosp.patients
    WHERE anchor_age >= 18
),

non_ckd_patients AS (
    SELECT DISTINCT subject_id
    FROM physionet-data.mimiciv_hosp.diagnoses_icd
    WHERE subject_id NOT IN (
        SELECT subject_id
        FROM physionet-data.mimiciv_hosp.diagnoses_icd
        WHERE LOWER(icd_code) LIKE '585%' -- CKD ICD-9 code
        AND icd_version = 9
    )
),

non_aki_patients AS (
    SELECT DISTINCT subject_id
    FROM physionet-data.mimiciv_hosp.diagnoses_icd
    WHERE subject_id NOT IN (
        SELECT subject_id
        FROM physionet-data.mimiciv_hosp.diagnoses_icd
        WHERE LOWER(icd_code) LIKE '584%' -- AKI ICD-9 code
        AND icd_version = 9
    )
),

creatinine_measurements AS (
    SELECT l.subject_id, l.hadm_id, l.charttime, l.valuenum
    FROM physionet-data.mimiciv_hosp.labevents l
    JOIN physionet-data.mimiciv_icu.icustays icu ON l.hadm_id = icu.hadm_id
    WHERE l.itemid = 50912 -- Creatinine item ID
    AND l.charttime <= TIMESTAMP_ADD(icu.intime, INTERVAL 72 HOUR)
),

patients_with_enough_creatinine_measurements AS (
    SELECT subject_id
    FROM creatinine_measurements
    GROUP BY subject_id
    HAVING COUNT(subject_id) >= 3
),

filtered_patients AS (
    SELECT adult_patients.subject_id, icu.intime
    FROM adult_patients
    JOIN non_ckd_patients USING (subject_id)
    JOIN non_aki_patients USING (subject_id)
    JOIN patients_with_enough_creatinine_measurements USING (subject_id)
    JOIN physionet-data.mimiciv_icu.icustays icu ON adult_patients.subject_id = icu.subject_id
),
physiological_data AS (
    SELECT
        f.subject_id,
        DATE_DIFF(l.charttime, f.intime, DAY) + 1 AS day,
        MAX(CASE WHEN l.itemid = 50912 THEN l.valuenum END) AS creatinine_max,
        MIN(CASE WHEN l.itemid = 50912 THEN l.valuenum END) AS creatinine_min,
        MAX(CASE WHEN l.itemid = 50809 THEN l.valuenum END) AS glucose_max,
        MIN(CASE WHEN l.itemid = 50809 THEN l.valuenum END) AS glucose_min,
        MAX(CASE WHEN l.itemid = 51222 THEN l.valuenum END) AS hemoglobin_max,
        MIN(CASE WHEN l.itemid = 51222 THEN l.valuenum END) AS hemoglobin_min,
        MAX(CASE WHEN l.itemid = 51265 THEN l.valuenum END) AS platelet_max,
        MIN(CASE WHEN l.itemid = 51265 THEN l.valuenum END) AS platelet_min,
        MAX(CASE WHEN l.itemid = 50971 THEN l.valuenum END) AS potassium_max,
        MIN(CASE WHEN l.itemid = 50971 THEN l.valuenum END) AS potassium_min,
        MAX(CASE WHEN l.itemid = 51275 THEN l.valuenum END) AS ptt_max,
        MIN(CASE WHEN l.itemid = 51275 THEN l.valuenum END) AS ptt_min,
        MAX(CASE WHEN l.itemid = 51237 THEN l.valuenum END) AS inr_max,
        MIN(CASE WHEN l.itemid = 51237 THEN l.valuenum END) AS inr_min,
        MAX(CASE WHEN l.itemid = 51006 THEN l.valuenum END) AS bun_max,
        MIN(CASE WHEN l.itemid = 51006 THEN l.valuenum END) AS bun_min,
        MAX(CASE WHEN l.itemid = 50893 THEN l.valuenum END) AS calcium_max,
        MIN(CASE WHEN l.itemid = 50893 THEN l.valuenum END) AS calcium_min,
        MAX(CASE WHEN l.itemid = 50825 THEN l.valuenum END) AS temp_max,
        MIN(CASE WHEN l.itemid = 50825 THEN l.valuenum END) AS temp_min
    FROM physionet-data.mimiciv_hosp.labevents l
    JOIN filtered_patients f ON l.subject_id = f.subject_id
    WHERE l.charttime <= TIMESTAMP_ADD(f.intime, INTERVAL 3 DAY)
    AND DATE_DIFF(l.charttime, f.intime, DAY) BETWEEN 0 AND 2
    GROUP BY f.subject_id, day
),
vital_signs_data AS (
    SELECT
        f.subject_id,
        DATE_DIFF(c.charttime, f.intime, DAY) + 1 AS day,
        MAX(CASE WHEN c.itemid = 220179 THEN c.valuenum END) AS systolic_bp_max,
        MIN(CASE WHEN c.itemid = 220179 THEN c.valuenum END) AS systolic_bp_min,
        MAX(CASE WHEN c.itemid = 220180 THEN c.valuenum END) AS diastolic_bp_max,
        MIN(CASE WHEN c.itemid = 220180 THEN c.valuenum END) AS diastolic_bp_min,
        MAX(CASE WHEN c.itemid = 220045 THEN c.valuenum END) AS heart_rate_max,
        MIN(CASE WHEN c.itemid = 220045 THEN c.valuenum END) AS heart_rate_min
    FROM physionet-data.mimiciv_icu.chartevents c
    JOIN filtered_patients f ON c.subject_id = f.subject_id
    WHERE c.charttime <= TIMESTAMP_ADD(f.intime, INTERVAL 3 DAY)
    AND DATE_DIFF(c.charttime, f.intime, DAY) BETWEEN 0 AND 2
    GROUP BY f.subject_id, day
),

patient_weight AS (
    SELECT
        subject_id,
        AVG(valuenum) AS weight 
    FROM `physionet-data.mimiciv_icu.chartevents`
    WHERE itemid = 226512 
    AND subject_id IN (SELECT subject_id FROM filtered_patients) 
    GROUP BY subject_id
),

urine_output_data AS (
    SELECT
        f.subject_id,
        AVG(CAST(c.value AS FLOAT64)) / w.weight AS avg_urine_output_per_kg 
    FROM physionet-data.mimiciv_icu.outputevents c
    JOIN filtered_patients f ON c.subject_id = f.subject_id
    JOIN patient_weight w ON f.subject_id = w.subject_id
    WHERE c.itemid IN (40055, 43175, 40069, 40094, 40715, 226559, 226560, 226561, 226563, 226564, 226565, 226567)
    AND DATE_DIFF(c.charttime, f.intime, DAY) = 0 
    GROUP BY f.subject_id, w.weight
),
days AS (
    SELECT 1 AS day UNION ALL
    SELECT 2 UNION ALL
    SELECT 3
),



urine_output AS (
    SELECT
        subject_id,
        charttime,
        value AS urine_output, 
        LEAD(charttime) OVER (PARTITION BY subject_id ORDER BY charttime) AS next_time, 
        TIMESTAMP_DIFF(LEAD(charttime) OVER (PARTITION BY subject_id ORDER BY charttime), charttime, HOUR) AS hours_between 
    FROM `physionet-data.mimiciv_icu.outputevents`
    WHERE itemid IN (40055, 43175, 40069, 40094, 40715, 226559, 226560, 226561, 226563, 226564, 226565, 226567) 
    AND subject_id IN (SELECT subject_id FROM filtered_patients) 
),


urine_output_per_kg_per_hour AS (
    SELECT
        u.subject_id,
        u.charttime,
        u.urine_output,
        w.weight,
        u.hours_between,
        (u.urine_output / (w.weight * u.hours_between)) AS urine_per_kg_per_hour 
    FROM urine_output u
    JOIN patient_weight w ON u.subject_id = w.subject_id
    WHERE u.hours_between > 0 
    AND u.subject_id IN (SELECT subject_id FROM filtered_patients) 
),


aki_patients AS (
    SELECT
        subject_id,
        COUNTIF(urine_per_kg_per_hour < 0.5) AS low_urine_hours 
    FROM urine_output_per_kg_per_hour
    GROUP BY subject_id
    HAVING low_urine_hours >= 6 
    AND subject_id IN (SELECT subject_id FROM filtered_patients) 
),
aki_status AS (
    SELECT
        p.subject_id,
        p.creatinine_min AS day1_creatinine_min,
        p2.creatinine_max AS day2_creatinine_max,
        p3.creatinine_max AS day3_creatinine_max,
        CASE
            WHEN (p2.creatinine_max - p.creatinine_min >= 0.3) OR (p3.creatinine_max - p.creatinine_min >= 0.3) THEN 'AKI'
            WHEN (p.creatinine_min IS NOT NULL AND p.creatinine_min != 0) AND 
                 ((p2.creatinine_max / NULLIF(p.creatinine_min, 0) >= 1.5) OR (p3.creatinine_max / NULLIF(p.creatinine_min, 0) >= 1.5)) THEN 'AKI'
            WHEN (p.subject_id IN (SELECT DISTINCT subject_id FROM aki_patients)) THEN 'AKI'
            ELSE 'Non-AKI'
        END AS aki_status
    FROM physiological_data p
    LEFT JOIN physiological_data p2 ON p.subject_id = p2.subject_id AND p2.day = 2
    LEFT JOIN physiological_data p3 ON p.subject_id = p3.subject_id AND p3.day = 3
    WHERE p.day = 1
    AND p.subject_id IN (SELECT subject_id FROM filtered_patients) 
)




-- Combine all data
SELECT DISTINCT
    f.subject_id,
    # d.day,
    p.creatinine_max,
    p.creatinine_min,
    v.systolic_bp_max,
    v.systolic_bp_min,
    v.diastolic_bp_max,
    v.diastolic_bp_min,
    v.heart_rate_max,
    v.heart_rate_min,
    p.glucose_max,
    p.glucose_min,
    p.hemoglobin_max,
    p.hemoglobin_min,
    p.platelet_max,
    p.platelet_min,
    p.potassium_max,
    p.potassium_min,
    p.ptt_max,
    p.ptt_min,
    p.inr_max,
    p.inr_min,
    p.bun_max,
    p.bun_min,
    p.calcium_max,
    p.calcium_min,
    p.temp_max,
    p.temp_min,
    u.avg_urine_output_per_kg,
    a.aki_status
FROM filtered_patients f
CROSS JOIN days d
LEFT JOIN physiological_data p ON f.subject_id = p.subject_id AND d.day = p.day
LEFT JOIN vital_signs_data v ON f.subject_id = v.subject_id AND d.day = v.day
LEFT JOIN urine_output_data u ON f.subject_id = u.subject_id 
LEFT JOIN aki_status a ON f.subject_id = a.subject_id
WHERE d.day=1
ORDER BY f.subject_id