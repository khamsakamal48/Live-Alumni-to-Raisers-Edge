# Use an official Python runtime as a parent image
FROM python:3-slim-buster

# Set environment variables
ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

## Set default values for PostgreSQL variables
ENV DB_IP=10.199.4.96
ENV DB_NAME=live_alumni_to_raisers_edge
ENV DB_USER=kamal
ENV DB_PASS=kamal1991

# Install postgresql-client to interact with the database
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    software-properties-common \
    git \
    libpq-dev python-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy the app code
COPY . /app
WORKDIR /app

# Install the dependencies
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port on which your Streamlit app will run
EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

# Run app.py when the container launches
CMD ["streamlit", "run", "🏠 Home.py"]
