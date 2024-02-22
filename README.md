# Live Alumni to Raisers Edge

This is a web-app developed on Python and mostly using streamlit library. It's objective is to let the user upload CSV 
files from the Blackbaud's Raisers Edge and Live Alumni application to essentially compare the data, identify the delta 
that is missing in Raisers Edge and let the user download this so that it can be uploaded to Raisers Edge using its 
Database view.

## Requirements
- Python 3.11 or higher
- Hosted PostgresSQL Database
- Docker/Podman to deploy and host the container

## Installation
- Clone this repository to your local machine:
```bash
git clone https://github.com/khamsakamal48/Live-Alumni-to-Raisers-Edge
```

## Deployment
- Change directory to the project folder:
```bash
cd Live-Alumni-to-Raisers-Edge
```
- Build the Docker image from the Dockerfile using the following command:
```bash
docker build -t Live-Alumni-to-Raisers-Edge .
```
- Run the image as a container
  - When using Docker
    ```bash
    docker run -p 8501:8501 -e DB_IP=host -e DB_USER=user -e DB_PASS=password -e DB_NAME=db Live-Alumni-to-Raisers-Edge
    ```
    - You need to replace the environment variables with your actual MySQL connection details.
  
  - When using Podman - Configure through the Host Server's Cockpit console 

## Usage
You can access the web service from your browser at http://localhost:8501/live-alumni.

