FROM base:latest

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

CMD ["faust", "worker", "-A", "enrichposts.app", "--web-port=5555", "--loglevel=info", "--broker=kafka://{{ .Release.Name }}-kafka:9092"]