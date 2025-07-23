''' script used for testing the class used to upload the CSV files into PostgreSQL '''
from data_upload import ClPostgresLoader
postgres_loader = ClPostgresLoader()
# postgres_loader.load_deps()
# postgres_loader.load_employees()
# postgres_loader.load_jobs()
postgres_loader.return_quarters()
