# syntax=docker/dockerfile:1.6
FROM node:20-alpine

ARG SDK_CONFIG
ARG REDIS_HOST
ARG REDIS_PORT
ARG REDIS_ID

WORKDIR /app

COPY package.json yarn.lock ./
# If you're in a monorepo, also COPY workspace package.json files needed for yarn resolution here.

# Use the secret npmrc only during install (not stored in layers)
RUN --mount=type=secret,id=npmrc,target=/root/.npmrc \
    yarn install --frozen-lockfile

# Now copy the rest
COPY . .

RUN yarn build

CMD ["yarn", "start-executor"]
