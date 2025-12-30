FROM astrocrpublic.azurecr.io/runtime:3.1-5

ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True 
ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True 
ENV AIRFLOW__API__ENABLE_CONNECTION_TEST=True 
ENV AIRFLOW__API__EXPOSE_CONFIG=True 

RUN pip install -r requirements.txt

RUN python -c "import duckdb; \
    con = duckdb.connect(); \
    con.execute('INSTALL ducklake; INSTALL postgres; INSTALL httpfs; INSTALL spatial;'); \
    con.close()"