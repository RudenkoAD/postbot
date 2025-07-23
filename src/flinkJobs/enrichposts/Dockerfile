FROM apache/flink:1.17-python

# Set working directory
WORKDIR /opt/flink_jobs/enrichposts

# Copy job code and requirements
COPY job.py ./
COPY requirements.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set the entrypoint for the job (can be overridden by k8s job spec)
ENTRYPOINT ["python", "job.py"]
