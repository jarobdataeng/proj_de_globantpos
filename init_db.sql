CREATE TABLE IF NOT EXISTS departments (
department_id INTEGER,
department_name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
job_id INTEGER,
job_title VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS employees (
employee_id INTEGER,
employee_name VARCHAR(100),
hire_date DATE NOT NULL,
department_id INTEGER,
job_id INTEGER
);

CREATE TABLE IF NOT EXISTS zemployees (
employee_id INTEGER,
employee_name VARCHAR(100),
hire_date TIMESTAMP NOT NULL,
department_id INTEGER,
job_id INTEGER
);