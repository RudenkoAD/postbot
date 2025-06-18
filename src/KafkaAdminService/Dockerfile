FROM python:3.12
WORKDIR /app

COPY ./KafkaAdminService/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./KafkaAdminService KafkaAdminService
COPY ./Commands Commands
COPY ./Utils Utils
COPY ./config.py .

ENV PYTHONPATH "${PYTHONPATH}:/app"

CMD ["python3", "KafkaAdminService/KafkaAdminManager.py"]
