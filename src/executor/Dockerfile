FROM node:18-alpine

ARG SDK_CONFIG
ARG REDIS_HOST
ARG REDIS_PORT

WORKDIR /app

COPY . .

RUN yarn install 

COPY . .

RUN yarn build

CMD yarn start-executor
