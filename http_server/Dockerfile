FROM python:3.12
WORKDIR /app
COPY . .
EXPOSE 8080
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r ./requirements.txt
CMD [ "python", "main.py" ]
