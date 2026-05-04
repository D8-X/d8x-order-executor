# syntax=docker/dockerfile:1.6

# ---- builder ----
FROM node:20-alpine AS builder
WORKDIR /app

COPY package.json yarn.lock ./

# Use the secret npmrc only during install (not stored in layers)
RUN --mount=type=secret,id=npmrc,target=/root/.npmrc \
    yarn install --frozen-lockfile

COPY . .

RUN yarn build && yarn bundle:sentinel

# ---- runtime ----
# Single bundled CJS file: no node_modules, no source, no SDK files on disk.
FROM node:20-alpine
WORKDIR /app

COPY --from=builder /app/bundles/sentinel.cjs ./sentinel.cjs

CMD ["node", "sentinel.cjs"]
