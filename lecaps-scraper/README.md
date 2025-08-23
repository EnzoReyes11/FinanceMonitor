# IAMC Report Scraper

This is a Python script that scrapes the latest financial report from the IAMC website, extracts data from a PDF, and serves it as a JSON response. The application is designed to be deployed as a container on Google Cloud Run.

## Prerequisites

- [Docker](https://www.docker.com/get-started) must be installed on your local machine.

## Building the Docker Image

First, navigate to the `lecaps-scraper-job` directory:

```sh
cd lecaps-scraper-job
```

Then, to build the Docker image, run the following command:

```sh
docker build -t lecap-scraper .
```

## Running the Docker Container

To run the Docker container locally, use the following command:

```sh
docker run -p 8080:8080 lecap-scraper
```

This will start the Flask application on port 8080 inside the container and map it to port 8080 on your host machine.

## Usage

Once the container is running, you can access the endpoint to get the report data.

### Using `curl`

```sh
curl http://localhost:8080/
```

### Using a Web Browser

Open your web browser and navigate to `http://localhost:8080/`.

You should receive a JSON response containing the extracted data from the latest report's PDF.

## Deployment to Google Cloud Run

To deploy this container to Google Cloud Run, you can use the Google Cloud SDK (`gcloud`).

1.  **Enable the required APIs**:
    ```sh
    gcloud services enable run.googleapis.com
    gcloud services enable containerregistry.googleapis.com
    ```

2.  **Authenticate with Google Cloud**:
    ```sh
    gcloud auth login
    gcloud auth configure-docker
    ```

3.  **Build and push the image to Google Container Registry (GCR)**:
    Replace `[PROJECT-ID]` with your Google Cloud project ID. First, navigate to the `lecaps-scraper-job` directory:
    ```sh
    cd lecaps-scraper-job
    ```
    Then, build and push the image:
    ```sh
    docker build -t gcr.io/[PROJECT-ID]/lecap-scraper .
    docker push gcr.io/[PROJECT-ID]/lecap-scraper
    ```

4.  **Deploy the image to Cloud Run**:
    Replace `[PROJECT-ID]` with your Google Cloud project ID and `[REGION]` with your desired region (e.g., `us-central1`).
    ```sh
    gcloud run deploy lecap-scraper \
      --image gcr.io/[PROJECT-ID]/lecap-scraper \
      --platform managed \
      --region [REGION] \
      --allow-unauthenticated
    ```

After deployment, Cloud Run will provide you with a URL to access your service.
