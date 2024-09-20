FROM apache/airflow:latest

USER root

# Install system dependencies and ODBC drivers
RUN apt-get update && apt-get install -y \
    mdbtools \
    unixodbc \
    unixodbc-dev \
    odbc-mdbtools \
    odbc-postgresql \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


# Create ODBC configuration files manually
RUN echo -e "[ODBC Data Sources]\nMDB=MDB Driver\n\n[MDB]\nDriver=/usr/lib/x86_64-linux-gnu/odbc/libmdbodbc.so\nDescription=MDB Driver\n\n[ODBC]\nTrace=Yes\nTraceFile=/tmp/odbc.log" > /etc/odbc.ini
RUN echo -e "[MDB]\nDescription=MDB Driver\nDriver=/usr/lib/x86_64-linux-gnu/odbc/libmdbodbc.so\nSetup=/usr/lib/x86_64-linux-gnu/odbc/libmdbodbc.so\nFileUsage=1\nFileExtns=*.mdb,*.accdb\n\n[PostgreSQL]\nDescription=PostgreSQL ODBC Driver\nDriver=/usr/lib/x86_64-linux-gnu/odbc/psqlodbcw.so" > /etc/odbcinst.ini

# Set permissions
RUN chmod 644 /etc/odbc.ini /etc/odbcinst.ini

USER airflow

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt
