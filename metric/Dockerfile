FROM python:3.9
WORKDIR /usr/src/app

COPY ./src/ ./src/
COPY ./requirements.txt ./

RUN pip install -r requirements.txt

CMD [ "python", "./src/metric.py" ]