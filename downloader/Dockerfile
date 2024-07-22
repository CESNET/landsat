FROM python:3.12
RUN useradd -m landsat_downloader
USER landsat_downloader
ENV PATH="/home/landsat_downloader/.local/bin:${PATH}"
WORKDIR /app
COPY ./requirements.txt .
RUN pip install --no-cache-dir -r ./requirements.txt
CMD [ "python", "main.py" ]
