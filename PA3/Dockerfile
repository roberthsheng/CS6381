# Use the official Python 3.12.3 slim image as a parent image
FROM python:3.12.3-slim

# Install build tools and dependencies required for building packages like netifaces
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

ENV PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python
# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install any dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application code into the container
COPY . .

# Expose any ports your application uses. For example:
EXPOSE 5555
EXPOSE 5556
EXPOSE 5560

# Set the default command. This can be overridden by the Kubernetes manifest or docker run.
CMD ["python3"]