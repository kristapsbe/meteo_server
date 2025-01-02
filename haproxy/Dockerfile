FROM alpine:3.21

WORKDIR /haproxy
COPY haproxy.cfg .

RUN apk update && apk upgrade && apk add --no-cache haproxy=3.0.7-r0

CMD ["haproxy", "-f", "/haproxy/haproxy.cfg"]
EXPOSE 443
