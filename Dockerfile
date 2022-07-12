FROM python:3.9

RUN apt-get update -y
RUN apt-get install -y nano jq
RUN pip install nose pymongo numpy pandas xarray netcdf4 geopy

WORKDIR /app
COPY . .
RUN chown -R 1000660000 /app