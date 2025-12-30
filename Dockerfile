FROM astrocrpublic.azurecr.io/runtime:3.1-5

# Instalar extensiones necesarias de DuckDB durante el build
# Esto asegura que las extensiones estén disponibles para todos los workers
RUN python -c "import duckdb; \
    con = duckdb.connect(); \
    con.execute('INSTALL ducklake; INSTALL postgres; INSTALL httpfs; INSTALL spatial;'); \
    con.close()"
