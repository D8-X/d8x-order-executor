# syntax=docker/dockerfile:1.6
FROM node:20-alpine

ARG SDK_CONFIG
ARG REDIS_HOST
ARG REDIS_PORT
ARG REDIS_ID

WORKDIR /app

COPY package.json yarn.lock ./
# If this is a monorepo, also COPY any workspace package.json files needed by yarn.

RUN --mount=type=secret,id=npmrc,target=/root/.npmrc \
    yarn install --frozen-lockfile

COPY . .

RUN yarn build

CMD ["yarn", "start-sentinel"]

