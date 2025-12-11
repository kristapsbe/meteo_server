FROM alpine:3.23

ARG HOST_ARCHITECTURE="unknown"
ARG DO_RUN_AURORA=false

ENV DO_RUN_AURORA=$DO_RUN_AURORA

WORKDIR /app
COPY . .
COPY utils/settings.example.py utils/settings.py

RUN apk update && \
    apk upgrade && \
    apk add --no-cache libc6-compat=1.1.0-r4 curl=8.17.0-r1 python3=3.12.12-r0 uv=0.9.16-r0

RUN [ "${HOST_ARCHITECTURE}" = "arm" ] &&\
    wget "https://github.com/nalgeon/sqlean/releases/download/0.27.1/sqlean-linux-arm64.zip" -O sqlean.zip ||\
    wget "https://github.com/nalgeon/sqlean/releases/download/0.27.1/sqlean-linux-x86.zip" -O sqlean.zip &&\
    unzip sqlean.zip -d /sqlite_extensions

RUN crontab -l > cron
RUN echo "0 */4 * * *	/app/run_job.sh download_large" >> cron
RUN echo "20,40 */4 * * *	/app/run_job.sh download_aurora" >> cron
RUN echo "*/20	1,2,3,5,6,7,9,10,11,13,14,15,17,18,19,21,22,23	*	*	*	/app/run_job.sh download_small" >> cron
RUN echo "30	*/4	*	*	*	/app/run_job.sh crawl_site" >> cron
RUN crontab cron

CMD ["sh", "start.sh"]
