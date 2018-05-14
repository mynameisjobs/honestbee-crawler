FROM alpine:3.7
WORKDIR /app
COPY requirements.txt .
RUN apk add --update --no-cache python3 postgresql-dev && \
      apk add --virtual build-deps gcc python3-dev musl-dev && \
      pip3 install -r requirements.txt && \
      apk del build-deps
COPY . .
