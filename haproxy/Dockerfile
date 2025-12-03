FROM alpine:3.23

WORKDIR /haproxy
COPY . .

RUN apk update && apk upgrade && apk add --no-cache haproxy=3.2.9-r0 python3=3.12.12-r0 uv=0.7.22-r0

RUN crontab -l > cron
RUN echo "0	0	1	*/2	*	/haproxy/renew_certs.sh &> /certs/cert_renewal.log" >> cron
RUN crontab cron

CMD ["sh", "start.sh"]
EXPOSE 80 443
