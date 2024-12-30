FROM python:3.13-rc-alpine

WORKDIR /app

RUN apk update && apk upgrade
#RUN pip install --no-cache-dir --upgrade pip \
#    && pip install --no-cache-dir -r requirements.txt

#CMD ["python", "main.py"]
EXPOSE 8000
