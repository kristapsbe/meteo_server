FROM alpine:3.21

USER root

WORKDIR /app
COPY . .
COPY settings.example.py settings.py

RUN apk update && apk upgrade && apk add --no-cache curl=8.11.1-r0 python3=3.12.8-r1 uv=0.5.6-r0

RUN curl -sS https://webi.sh/sqlpkg | sh
RUN ~/.local/bin/sqlpkg install nalgeon/fuzzy # sqlite/spellfix isn't available for linux arm64
RUN sed -i "s~{editdist_extension}~$(~/.local/bin/sqlpkg which nalgeon/fuzzy)~" settings.py

CMD ["uv", "run", "main.py"]
EXPOSE 8000
