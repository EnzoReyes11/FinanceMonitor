# Use an official Python runtime as a parent image.
# Choose a version compatible with your code and GCF supported runtimes (e.g., 3.9, 3.10, 3.11).
FROM python:3.10-slim

# Set environment variables for the Functions Framework.
# GOOGLE_FUNCTION_TARGET should match the name of your entry point function in main.py.
ENV GOOGLE_FUNCTION_TARGET=main
ENV GOOGLE_FUNCTION_SIGNATURE_TYPE=http
ENV GOOGLE_FUNCTION_SOURCE=main.py
# The Functions Framework listens on port 8080 by default. Cloud Run will map to this.
ENV PORT=8080

# Set the working directory in the container.
WORKDIR /app

# Copy the requirements file into the container.
COPY requirements.txt .

# Install dependencies.
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY iol.py .
COPY alphavantage.py .
COPY bq.py .

# Make port 8080 available to the world outside this container (Cloud Run default)
EXPOSE 8080

# Run the Functions Framework when the container starts.
# This will start a web server that invokes your function.
#CMD exec functions-framework --target=${GOOGLE_FUNCTION_TARGET} --signature-type=${GOOGLE_FUNCTION_SIGNATURE_TYPE} --port=${PORT}

#Run main:app when the container launches using Gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "2", "main:app"]