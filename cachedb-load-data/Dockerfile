# from python
FROM python:3.9.1-slim-buster

ENV POSTGRES_USR="cachedb_user"
ENV POSTGRES_DB="cachedb"
ENV POSTGRES_PW="cachedb_pass"
ENV POSTGRES_HST="localhost"
ENV DATA_PATH="data"

COPY requirements.txt create_db.py load_data.py /loader/
COPY data/ /loader/data/
# set working directory
WORKDIR /loader

# install python requirements
RUN pip install -r requirements.txt

# run the script
CMD ["python", "create_db.py"]
CMD ["python", "load_data.py"]