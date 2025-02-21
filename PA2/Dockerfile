# Use the official Python 3.12.3 slim image as a parent image
FROM python:3.12.3-slim

# Install build tools, Java (required for ZooKeeper), and other dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        build-essential \
        wget \
        tar \
        default-jre-headless \
    && rm -rf /var/lib/apt/lists/*


ENV PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install Python dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application code into the container
COPY . .

# (Optional) Install Apache ZooKeeper 3.8.4 for testing purposes.
# In production, you would run ZooKeeper as a separate service.
# Install Apache ZooKeeper 3.8.4
# ENV ZK_VERSION=3.8.4
# ENV ZK_HOME=/opt/zookeeper
# RUN mkdir -p /tmp/zookeeper

# RUN wget https://downloads.apache.org/zookeeper/zookeeper-${ZK_VERSION}/apache-zookeeper-${ZK_VERSION}-bin.tar.gz \
#     && tar xvzf apache-zookeeper-${ZK_VERSION}-bin.tar.gz -C /opt/ \
#     && mv /opt/apache-zookeeper-${ZK_VERSION}-bin ${ZK_HOME} \
#     && rm apache-zookeeper-${ZK_VERSION}-bin.tar.gz \
#     # Copy the sample configuration to the expected file
#     && cp ${ZK_HOME}/conf/zoo_sample.cfg ${ZK_HOME}/conf/zoo.cfg


# # Add ZooKeeper's bin directory to PATH (for local testing or manual invocation)
# ENV PATH="${ZK_HOME}/bin:${PATH}"

# Expose the ports your application uses and ZooKeeper's default port (if needed)
EXPOSE 5555
EXPOSE 5556
EXPOSE 5560
EXPOSE 2181

