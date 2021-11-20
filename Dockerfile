FROM python:3.9.7-alpine
WORKDIR /app

COPY . .

RUN apk update && pip install -U pip
RUN apk add --virtual .build-deps gcc python3-dev musl-dev postgresql-dev linux-headers libffi-dev jpeg-dev zlib-dev curl

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

CMD python movies_admin/manage.py runserver 127.0.0.1:9000