# Use a lightweight Python image as the base image
FROM python:3.9-slim

# Install Rust and necessary system dependencies
RUN apt update && apt install -y curl gcc g++ libpq-dev cargo

# Ensure Rust is in PATH
ENV PATH="/root/.cargo/bin:${PATH}"

# Upgrade pip and install pydantic_core without compilation
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --only-binary=:all: "pydantic_core<2.10" || \
    pip install --no-cache-dir pydantic_core

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements.txt file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire application into the container
COPY . .

# Run the application with Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0"]
