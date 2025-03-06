# Use the official Python 3.10 image as the base image
FROM python:3.10

# Set the working directory inside the container
WORKDIR /app

# Install necessary system dependencies
RUN apt update && apt install -y curl gcc g++ libpq-dev && \
    curl https://sh.rustup.rs -sSf | sh -s -- -y && \
    export PATH="/root/.cargo/bin:$PATH" && \
    rustc --version

# Set the environment variable for Rust
ENV PATH="/root/.cargo/bin:$PATH"

# Upgrade pip to the latest version
RUN pip install --upgrade pip

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire application into the container
COPY . .

# Expose the default FastAPI port
EXPOSE 8000

# Run the application with Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
