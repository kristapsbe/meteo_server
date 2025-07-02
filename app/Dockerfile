FROM alpine:3.22

ARG HOST_ARCHITECTURE="unknown"

WORKDIR /app
COPY . .
COPY utils/settings.example.py utils/settings.py

RUN apk update && \
    apk upgrade && \
    apk add --no-cache libc6-compat=1.1.0-r4 curl=8.14.1-r1 python3=3.12.11-r0 uv=0.7.9-r0

RUN [ "${HOST_ARCHITECTURE}" = "arm" ] &&\
    wget "https://github.com/nalgeon/sqlean/releases/download/0.27.1/sqlean-linux-arm64.zip" -O sqlean.zip ||\
    wget "https://github.com/nalgeon/sqlean/releases/download/0.27.1/sqlean-linux-x86.zip" -O sqlean.zip &&\
    unzip sqlean.zip -d /sqlite_extensions

RUN crontab -l > cron
RUN echo "*/20	*	*	*	*	/app/run_job.sh download" >> cron
RUN echo "5	*/4	*	*	*	/app/run_job.sh crawl_site" >> cron
RUN crontab cron

CMD ["sh", "start.sh"]
