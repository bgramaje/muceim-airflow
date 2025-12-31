FROM python:3.11-slim

# Instalar dependencias del sistema necesarias para DuckDB
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Establecer directorio de trabajo
WORKDIR /app

# Copiar requirements del nivel superior e instalar dependencias
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Instalar extensiones de DuckDB
RUN python -c "import duckdb; \
    con = duckdb.connect(); \
    con.execute('INSTALL ducklake; INSTALL postgres; INSTALL httpfs;'); \
    con.close()"

# Copiar código de la aplicación
COPY main.py .

# Ejecutar el job
CMD ["python", "main.py"]

