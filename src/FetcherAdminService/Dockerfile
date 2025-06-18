FROM python:3.12
WORKDIR /app

COPY ./FetcherAdminService/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./FetcherAdminService FetcherAdminService
COPY ./Commands Commands
COPY ./Utils Utils
COPY ./config.py .

ENV PYTHONPATH "${PYTHONPATH}:/app"

CMD ["python3", "FetcherAdminService/FetcherAdminManager.py"]
