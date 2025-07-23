WITH cte_hires AS (
  SELECT
    d.department_name AS department,
    EXTRACT(YEAR FROM e.hire_date) AS hire_year,
    COUNT(*) FILTER (WHERE EXTRACT(QUARTER FROM e.hire_date) = 1) AS q1,
    COUNT(*) FILTER (WHERE EXTRACT(QUARTER FROM e.hire_date) = 2) AS q2,
    COUNT(*) FILTER (WHERE EXTRACT(QUARTER FROM e.hire_date) = 3) AS q3,
    COUNT(*) FILTER (WHERE EXTRACT(QUARTER FROM e.hire_date) = 4) AS q4
  FROM zemployees e
  INNER JOIN departments d ON d.department_id = e.department_id
  GROUP BY d.department_name, EXTRACT(YEAR FROM e.hire_date)
)
SELECT
  department,
  hire_year,
  q1,
  q2,
  q3,
  q4,
  (q1 + q2 + q3 + q4) AS total
FROM cte_hires
ORDER BY total DESC
LIMIT 3;

WITH dept_hires AS (
    SELECT
        d.department_id,
        d.department_name,
        COUNT(e.employee_id) AS total_hired
    FROM zemployees e
    INNER JOIN departments d ON d.department_id = e.department_id
    WHERE EXTRACT(YEAR FROM e.hire_date) = 2021
    GROUP BY d.department_id, d.department_name
),
avg_hires AS (
    SELECT AVG(total_hired) AS mean_hired FROM dept_hires
)
SELECT
    department_id AS id,
    department_name AS department,
    total_hired AS hired
FROM dept_hires
WHERE total_hired > (SELECT mean_hired FROM avg_hires)
ORDER BY total_hired DESC;
