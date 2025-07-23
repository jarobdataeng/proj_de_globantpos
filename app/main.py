''' script used as the core baseline '''
from fastapi import FastAPI
from app.data_upload import ClPostgresLoader

app = FastAPI()

@app.on_event("startup")
def startup_event():
    postgres_loader = ClPostgresLoader()
    postgres_loader.load_deps()
    postgres_loader.load_jobs()
    postgres_loader.load_zemployees()

    # Store the result in app.state
    app.state.quarter_query = postgres_loader.return_quarters()

@app.get("/")
def root():
    return {
        "message": "API is up and running, and the data was uploaded",
        "quarter_summary": app.state.quarter_query
    }

@app.get("/quarters")
def get_quarter_summary():
    return {
        "quarters": app.state.quarter_query
    }
