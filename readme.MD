# Project: Global Challenge
##  Project Solution Architecture
proj_de_globantpos/
├── app/
│   ├── __init__.py
│   ├── main.py
│   ├── database.py
│   ├── models.py
│   ├── schemas.py
│   ├── crud.py
│   └── routers/
│       ├── __init__.py
│       └── upload.py
├── data/
│   ├── jobs.csv
│   ├── departments.csv
│   └── hired_employees.csv
├── Dockerfile
├── docker-compose.yml
└── requirements.txt

## Setup
docker-compose up --build
docker exec -it proj_de_globantpos-pg-1  psql -U myuser -d mydatabase;