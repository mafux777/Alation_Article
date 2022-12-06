FROM python:3.10-rc-slim-bullseye
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip3 install -r requirements.txt
