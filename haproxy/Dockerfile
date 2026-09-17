FROM python:3.14-alpine3.23

WORKDIR /haproxy
COPY . .

RUN apk update && apk upgrade && apk add --no-cache haproxy=3.4.4-r0 uv=0.10.2-r0

RUN uv sync --frozen

RUN crontab -l > cron
RUN echo "17	3	*	*	*	/haproxy/renew_certs.sh" >> cron
RUN crontab cron

CMD ["sh", "/haproxy/start.sh"]
EXPOSE 80 443
