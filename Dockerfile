FROM apache/airflow:2.8.1

# Specify additional Python packages you want to install
#!/bin/bash
ARG SMTP_USER
ARG SMTP_PW

RUN echo $SMTP_USER
RUN echo $SMTP_PW
ENV SMTP_USER=$SMTP_USER
ENV SMTP_PW=$SMTP_PW
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt
