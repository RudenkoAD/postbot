FROM python:3.12
WORKDIR /app

COPY ./FetchingServices/VKFetchingService/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./FetchingServices/VKFetchingService FetchingServices/VKFetchingService
COPY ./FetchingServices FetchingServices
COPY ./Commands Commands
COPY ./Utils Utils
COPY ./config.py .

ENV PYTHONPATH "${PYTHONPATH}:/app"

CMD ["python3", "./FetchingServices/VKFetchingService/VKFetchingManager.py"]
