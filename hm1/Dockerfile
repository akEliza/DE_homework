FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_data.py ingest_data.py
COPY green_tripdata_2019-10.csv green_tripdata_2019-10.csv
COPY taxi_zone_lookup.csv taxi_zone_lookup.csv

# ENTRYPOINT ["bash"]
# ENTRYPOINT ["python", "ingest_data.py", \
    # "--user=root", \
    # "--password=root", \
    # "--host=localhost", \
    # "--port=5432", \
    # "--db=ny_taxi", \
    # "--table_name=green_trip_data", \
    # "--csv_path=/app/green_tripdata_2019-10.csv"]