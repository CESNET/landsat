FROM python:3.12
RUN useradd -m landsat_http_server
USER landsat_http_server
ENV PATH="/home/landsat_http_server/.local/bin:${PATH}"
WORKDIR /app
COPY . .
EXPOSE 8080
RUN pip install --no-cache-dir -r ./requirements.txt
CMD [ "python", "main.py" ]
