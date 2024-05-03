FROM apache/airflow:latest

USER root

# Update packages and install necessary libraries
RUN apt-get update && \
    apt-get -y install git && \
    apt-get -y install build-essential && \
    apt-get -y install python3-dev && \
    apt-get -y install libatlas-base-dev && \
    apt-get -y install libopenblas-dev && \
    apt-get clean

# Switch back to the airflow user
USER airflow

# Install required Python libraries
RUN pip install --no-cache-dir \
    pandas \
    numpy \
    scikit-learn \
    plotly \
    logging \
    && \
    pip install --upgrade pip setuptools wheel


# Set the working directory
WORKDIR /opt/airflow





