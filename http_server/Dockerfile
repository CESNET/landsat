FROM python:latest
WORKDIR /app
COPY . .
EXPOSE 8080
RUN pip install --no-cache-dir -r requirements.txt
CMD [ "python", "main.py" ]
