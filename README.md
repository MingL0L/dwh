# Data Warehouse
The object of this project is to create a Data Warehouse using Redshift.
And tranform Webstat_Export_20210430.csv into dimension and fact tables.
### Step 1: Achitecture Data Warehouse
![Tux, the Linux mascot](pic/etl.png)
### Step 2: Schema
![Tux, the Linux mascot](pic/schema.PNG)
### Step 3: DAG
![Tux, the Linux mascot](pic/dag.PNG)
### Step 4: Setup
- Build docker image
```
docker build -t apache/airflow:2.0.1-fixed
```

- Start Airflow
```
docker-compose up
```
- Open `localhost:8080` from browser.
- Click on the Admin tab and select Connections 
  - Conn Id: Enter aws_credentials.
  - Conn Type: Enter Amazon Web Services. 
  - Login: Enter your Access key ID from the IAM User credentials you downloaded earlier.
  - Password: Enter your Secret access key from the IAM User credentials you downloaded earlier.
### Step 5: Run
Toggle to trun on *dwh* dag, and the pipline will run on a daily base.
![Tux, the Linux mascot](pic/run.PNG)
