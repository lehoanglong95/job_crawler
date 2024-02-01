FROM apache/airflow:2.8.1

# Specify additional Python packages you want to install
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt
