FROM python:3.8

WORKDIR /app

COPY . .

RUN pip3 install psycopg2 python-dotenv tabulate
