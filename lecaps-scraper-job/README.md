# IAMC Report Scraper

This is a Python script that scrapes the latest financial report from the IAMC website, extracts data from a PDF, and serves it as a JSON response. The application is designed to be deployed as a container on Google Cloud Run.

[SOURCE](https://www.iamc.com.ar/informeslecap/)

## Prerequisites

- [Docker](https://www.docker.com/get-started) must be installed on your local machine.
- [Terraform](https://www.terraform.io/downloads.html) (for setting up alerting).

## Building the Docker Image

First, navigate to the `lecaps-scraper-job` directory:

```sh
cd lecaps-scraper-job
```

Then, to build the Docker image, run the following command:

```sh
docker build -t lecaps-scraper-local:latest .
```

## Running the Docker Container

To run the Docker container locally, use the following command:

```sh
docker run --env-file=.env -p 8080:8080 lecaps-scraper-local
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
    gcloud services enable bigquery.googleapis.com
    ```

2.  **Authenticate with Google Cloud**:
    ```sh
    gcloud auth login
    gcloud auth configure-docker
    ```

3.  **Build and push the image to Google Container Registry (GCR)**:
    Replace `[PROJECT-ID]` with your Google Cloud project ID. First, navigate to the `lecaps-scraper` directory:
    ```sh
    cd lecaps-scraper-job
    ```
    Then, build and push the image:
    ```sh
    docker build -t gcr.io/[PROJECT-ID]/lecaps-scraper .
    docker push gcr.io/[PROJECT-ID]/lecaps-scraper
    ```

4.  **Deploy the image to Cloud Run**:
    ```sh
    gcloud run deploy lecaps-scraper \
      --image gcr.io/[PROJECT-ID]/lecaps-scraper \
      --platform managed \
      --region [REGION] \
    ```

After deployment, Cloud Run will provide you with a URL to access your service.

## GCP Alerting with Terraform

This project includes a Terraform script to set up a log-based alert in your GCP project. This alert will monitor the logs from the `lecaps-scraper` Cloud Run service and notify you via email if any errors are detected.

### Prerequisites

- [Terraform](https://www.terraform.io/downloads.html) is installed.
- You have authenticated with GCP and have the necessary permissions to create monitoring resources.

### Setup

1.  **Navigate to the `lecaps-scraper-job` directory:**
    ```sh
    cd lecaps-scraper-job
    ```

2.  **Create a `terraform.tfvars` file** with the following content, replacing the placeholder values with your own:
    ```
    project_id         = "your-gcp-project-id"
    notification_email = "your-email@example.com"
    ```

3.  **Initialize Terraform:**
    ```sh
    terraform init
    ```

4.  **Apply the Terraform configuration:**
    ```sh
    terraform apply
    ```

This will create the following resources in your GCP project:
- A log-based metric to count errors from the `lecaps-scraper` service.
- An email notification channel.
- A monitoring alert policy that triggers when errors are detected.

