FROM alpine:3.21

USER root

WORKDIR /app
COPY . .
COPY utils/settings.example.py utils/settings.py

RUN apk update && \
    apk upgrade && \
    apk add --no-cache libc6-compat=1.1.0-r4 curl=8.11.1-r0 python3=3.12.8-r1 uv=0.5.6-r0

RUN curl -sS https://webi.sh/sqlpkg | sh
RUN ~/.local/bin/sqlpkg install nalgeon/fuzzy # sqlite/spellfix isn't available for linux arm64
RUN sed -i "s~{editdist_extension}~$(~/.local/bin/sqlpkg which nalgeon/fuzzy)~" utils/settings.py

RUN crontab -l > cron
RUN echo "*/20	*	*	*	*	/app/run_job.sh download" >> cron
RUN echo "10	*/4	*	*	*	/app/run_job.sh crawl_site" >> cron # need an empty line at the end of the file
RUN crontab cron

CMD ["sh", "start.sh"]
EXPOSE 8000
