FROM python:3.15-alpine3.24

WORKDIR /haproxy
COPY . .

RUN apk update && apk upgrade && apk add --no-cache haproxy=3.4.7-r0 uv=0.11.19-r2

RUN uv sync --frozen

RUN crontab -l > cron
RUN echo "17	3	*	*	*	/haproxy/renew_certs.sh" >> cron
RUN crontab cron

CMD ["sh", "/haproxy/start.sh"]
EXPOSE 80 443
