FROM alpine:3.21

ARG HOST_ARCHITECTURE="unknown"

USER root

WORKDIR /app
COPY . .
COPY utils/settings.example.py utils/settings.py

RUN apk update && \
    apk upgrade && \
    apk add --no-cache libc6-compat=1.1.0-r4 curl=8.11.1-r0 python3=3.12.8-r1 uv=0.5.6-r0

RUN [ "${HOST_ARCHITECTURE}" = "arm" ] &&\
    wget "https://github.com/nalgeon/sqlpkg-cli/releases/download/0.2.3/sqlpkg-cli_0.2.3_linux_arm64.tar.gz" -O sqlpkg.tar.gz ||\
    wget "https://github.com/nalgeon/sqlpkg-cli/releases/download/0.2.3/sqlpkg-cli_0.2.3_linux_amd64.tar.gz" -O sqlpkg.tar.gz &&\
    tar x -z -v -f sqlpkg.tar.gz &&\
    /app/sqlpkg install nalgeon/fuzzy &&\
    sed -i "s~{editdist_extension}~$(/app/sqlpkg which nalgeon/fuzzy)~" utils/settings.py

RUN crontab -l > cron
RUN echo "*/20	*	*	*	*	/app/run_job.sh download" >> cron
RUN echo "5	*/4	*	*	*	/app/run_job.sh crawl_site" >> cron
RUN crontab cron

RUN echo $UPTIMEROBOT

CMD ["sh", "start.sh"]
