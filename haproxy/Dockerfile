FROM alpine:3.22

WORKDIR /haproxy
COPY . .

RUN apk update && apk upgrade && apk add --no-cache haproxy=3.2.2-r0 python3=3.12.11-r0 uv=0.7.9-r0

RUN crontab -l > cron
RUN echo "0	0	1	*/2	*	/haproxy/renew_certs.sh" >> cron
RUN crontab cron

CMD ["sh", "start.sh"]
EXPOSE 80 443
