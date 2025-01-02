FROM alpine:3.21

WORKDIR /haproxy
COPY . .

RUN apk update && apk upgrade && apk add --no-cache haproxy=3.0.7-r0 python3=3.12.8-r1 uv=0.5.6-r0

RUN crontab -l > cron
RUN echo "0	0	1	*/2	*	/haproxy/renew_certs.sh" >> cron
RUN crontab cron

CMD ["sh", "start.sh"]
EXPOSE 80 443
