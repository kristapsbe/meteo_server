FROM python:3.14-alpine3.23

WORKDIR /haproxy
COPY . .

RUN apk update && apk upgrade && apk add --no-cache haproxy=3.2.19-r0 uv=0.10.2-r0

RUN crontab -l > cron
RUN echo "0	0	1	*/2	*	/haproxy/renew_certs.sh > /certs/cert_renewal.log 2>&1" >> cron
RUN crontab cron

CMD ["sh", "/haproxy/start.sh"]
EXPOSE 80 443
