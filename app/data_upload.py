''' script used to load the collected CSV files into a Postgres SQL DB '''
import os
import json
import logging
import glob
from datetime import datetime
import pandas as pd
import psycopg2 # type: ignore

# Read environment variables directly
pg_uid = os.getenv('POSTGRES_UID')
pg_pwd = os.getenv('POSTGRES_PWD')
pg_dbs = os.getenv('POSTGRES_DBS')
pg_hst = os.getenv('POSTGRES_HST')
pg_por = os.getenv('POSTGRES_POR')

if pg_uid is None:
    DATABASE_URL = 'postgresql://myuser:mypassword@pg:5432/mydatabase'
    logger = logging.getLogger(__name__)
    print("PG INFO: the user was not collected from the environment variables")
else:
    # Build the DATABASE_URL dynamically
    DATABASE_URL = f'postgresql://{pg_uid}:{pg_pwd}@{pg_hst}:{pg_por}/{pg_dbs}'
    logger = logging.getLogger(__name__)
    print("PG INFO: the user was founded--> %s", DATABASE_URL)

class ClPostgresLoader:
    ''' this class is used to upload data into Postgres DB'''
    def __init__(self):
        ''' init the class '''
        self.current_month = datetime.now().strftime('%Y%m')
        self.file_path = 'data'

    def load_deps(self):
        ''' load the departments stored into the csv file'''
        logging.info("loading depts into the postgres database...")
        try:
            # Define the current period
            f_path = self.file_path
            import os
            file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "departments.csv"))
            df = pd.read_csv(file_path, sep=",")
            # select the columns to be inserted into the table
            df1 = df[['department_id', 'department_name']]
            # Connect to the database
            conn = psycopg2.connect(DATABASE_URL)
            cursor = conn.cursor()
            truncate_sql = "truncate table departments;"
            cursor.execute(truncate_sql)
            # Insert data into the table
            for index, row in df1.iterrows():
                try:
                    s1 = row['department_id']
                    s2 = row['department_name']
                    # Insert the query
                    insert_query = f"""
                        INSERT INTO departments (
                            department_id, department_name
                        ) VALUES ('{s1}', '{s2}');
                        """
                    # print(f"department item loaded, {index}")
                    cursor.execute(insert_query)
                    conn.commit()
                except ImportError as e:
                    logging.info("error while trying to load a record %s:%s", s1, e)
                    continue
            # End of the loop: check out the results
            sql_query = "select count(1) as cnt from departments;"
            # Execute the query in Postgres SQL DB
            sql_result = cursor.execute(sql_query)
            cursor.close()
            conn.close()
            logging.info("loading departments into the postgres database completed.")
        except ImportError as e:
            print(f"an error occurred while loading departments: {e}")

    def load_jobs(self):
        ''' load the jobs stored into the csv file'''
        logging.info("loading jobs into the postgres database...")
        try:
            # Define the current period
            f_path = self.file_path
            import os
            file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "jobs.csv"))
            df = pd.read_csv(file_path, sep=",")
            # select the columns to be inserted into the table
            df1 = df[['job_id', 'job_title']]
            # Connect to the database
            conn = psycopg2.connect(DATABASE_URL)
            cursor = conn.cursor()
            truncate_sql = "truncate table jobs;"
            cursor.execute(truncate_sql)
            # Insert data into the table
            for index, row in df1.iterrows():
                try:
                    s1 = row['job_id']
                    s2 = row['job_title']
                    # Insert the query
                    insert_query = f"""
                        INSERT INTO jobs (
                            job_id, job_title
                        ) VALUES ('{s1}', '{s2}');
                        """
                    # print(f"job item loaded, {index}")
                    cursor.execute(insert_query)
                    conn.commit()
                except ImportError as e:
                    logging.info("error while trying to load a record %s:%s", s1, e)
                    continue
            # End of the loop: check out the results
            sql_query = "select count(1) as cnt from jobs;"
            # Execute the query in Postgres SQL DB
            sql_result = cursor.execute(sql_query)
            cursor.close()
            conn.close()
            logging.info("loading jobs into the postgres database completed.")
        except ImportError as e:
            print(f"an error occurred while loading jobs: {e}")

    def load_employees(self):
        ''' load the employees stored into the csv file'''
        logging.info("loading employees into the postgres database...")
        try:
            # Define the current period
            f_path = self.file_path
            import os
            file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "hired_employees.csv"))
            df = pd.read_csv(file_path, sep=",")
            # select the columns to be inserted into the table
            df1 = df[['employee_id','employee_name',
                      'hire_date','department_id','job_id']]

            df1['employee_name'] =  df['employee_name'].str.replace("'", "", regex=False)
            df1['hire_date'] =  df['hire_date'].fillna('1900-01-01T12:00:01Z')
            df1['job_id'] =  df1['job_id'].apply(lambda x: 0 if pd.isna(x) else x)
            df1['department_id'] =  df1['department_id'].apply(lambda x: 0 if pd.isna(x) else x)
          
            # Connect to the database
            conn = psycopg2.connect(DATABASE_URL)
            cursor = conn.cursor()
            truncate_sql = "truncate table employees;"
            cursor.execute(truncate_sql)
            # Insert data into the table
            for index, row in df1.iterrows():
                try:
                    s1 = row['employee_id']
                    s2 = row['employee_name']
                    s3 = row['hire_date']
                    s4 = row['department_id']
                    s5 = row['job_id']
                    
                    # Insert the query
                    insert_query = f"""
                        INSERT INTO employees (
                            employee_id,employee_name,hire_date,department_id,job_id
                        ) VALUES ('{s1}', '{s2}','{s3}', {s4}, {s5});
                        """
                    # print(f"employee item loaded, {index}")
                    cursor.execute(insert_query)
                    conn.commit()
                except ImportError as e:
                    logging.info("error while trying to load a record %s:%s", s1, e)
                    continue
            # End of the loop: check out the results
            sql_query = "select count(1) as cnt from employees;"
            # Execute the query in Postgres SQL DB
            sql_result = cursor.execute(sql_query)
            cursor.close()
            conn.close()
            logging.info("loading employees into the postgres database completed.")
        except ImportError as e:
            print(f"an error occurred while loading employees: {e}")

    def load_zemployees(self):
        ''' load the employees stored into the csv file'''
        logging.info("loading employees into the postgres database...")
        try:
            # Define the current period
            f_path = self.file_path
            import os
            file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "hired_employees.csv"))
            df = pd.read_csv(file_path, sep=",")
            # select the columns to be inserted into the table
            df1 = df[['employee_id','employee_name',
                      'hire_date','department_id','job_id']]

            df1['employee_name'] =  df['employee_name'].str.replace("'", "", regex=False)
            df1['hire_date'] =  df['hire_date'].fillna('1900-01-01T12:00:01Z')
            df1['job_id'] =  df1['job_id'].apply(lambda x: 0 if pd.isna(x) else x)
            df1['department_id'] =  df1['department_id'].apply(lambda x: 0 if pd.isna(x) else x)
          
            # Connect to the database
            conn = psycopg2.connect(DATABASE_URL)
            cursor = conn.cursor()
            truncate_sql = "truncate table zemployees;"
            cursor.execute(truncate_sql)
            # Insert data into the table
            for index, row in df1.iterrows():
                try:
                    s1 = row['employee_id']
                    s2 = row['employee_name']
                    s3 = row['hire_date']
                    s4 = row['department_id']
                    s5 = row['job_id']
                    
                    # Insert the query
                    insert_query = f"""
                        INSERT INTO zemployees (
                            employee_id,employee_name,hire_date,department_id,job_id
                        ) VALUES ('{s1}', '{s2}','{s3}', {s4}, {s5});
                        """
                    # print(f"employee item loaded, {index}")
                    cursor.execute(insert_query)
                    conn.commit()
                except ImportError as e:
                    logging.info("error while trying to load a record %s:%s", s1, e)
                    continue
            # End of the loop: check out the results
            sql_query = "select count(1) as cnt from employees;"
            # Execute the query in Postgres SQL DB
            sql_result = cursor.execute(sql_query)
            cursor.close()
            conn.close()
            logging.info("loading zemployees into the postgres database completed.")
        except ImportError as e:
            print(f"an error occurred while loading zemployees: {e}")

    def return_quarters(self):
        ''' return the quarter query '''
        logging.info("run the quarter query in the postgres database...")
        try:
            # Define the current period
            f_path = self.file_path
            import os
            
            # Connect to the database
            conn = psycopg2.connect(DATABASE_URL)
            cursor = conn.cursor()
            qry= """
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
            """
            cursor.execute(qry)
            # Fetch all rows
            rows = cursor.fetchall()

            # Convert to list of dicts
            json_result = json.dumps(rows, default=str)
            results = json_result
            
        except ImportError as e:
            print(f"an error occurred while running the quarter query: {e}")
            results = {"message": "the quarter query failed"}
        return results   
