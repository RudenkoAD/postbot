FROM base:latest
WORKDIR /app
ENV PYTHONUNBUFFERED=1
ENV PYTHONBUFFERED=0

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
CMD ["python3", "main.py"]
