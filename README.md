# D8X Order Executor

## 1 - Getting started

### Dependencies

These services can be run using [Docker](https://docs.docker.com/get-docker/).

In addition, you will need:

- A mnemonic seed phrase, used to generate private keys for the wallets that will execute orders
- RPCs (HTTP and WebSockets) to interact with the blockchain
- Optional: a broker WebSocket URL to speed-up execution of orders coming from a specific frontend (see the trader-backend setup for details)
- Optional: number of different servers running these bots, if applicable

The first address along the seed phrase's derivation path must be funded with with sufficient native tokens to execute orders. For instance, if running on Polygon zkEVM, the first address needs to have sufficient ETH to pay for gas, which is typically no more than `$10` per perpetual.

Note that your proceeds are sent to an address of your choice, which need not be associated with the provided mnemonic seed. Hence your earnings are not at risk even if the server is compromised. However, we do strongly encourage you to use a sufficiently secure network configuration when choosing a server.

## 2 - Configuration

### Environment variables

Rename the file `sample.env` as `.env` and edit as necessary:

These variables have sensible default values, but can be modified if needed:

- ACCOUNTS_PER_BOT: How many wallets will each bot be using.
  - More wallets allows faster execution with higher trading volume, but will require more gas tokens to be held in the seed phrase's wallet
- REDIS_URL
- REDIS_PASSWORD

These variables depend on your setup:

- CHAIN_ID: The chain ID of the network where orders are executed
- PEER_COUNT: Number of servers running bots. Defaults to 1 (single-server setup)
- PEER_INDEX: 0-indexed identifier, unique for each server. Defaults to 0 (single-server setup)
- EARNINGS_WALLET: Address of the wallet that will collect all earnings from order execution
- SEED_PHRASE: Your mnemonic seed phrase.
  - Remember to fund the first address along the derivation path.
  - You can create a seed phrase by creating a new Metamask wallet, exporting the seed phrase, and funding the first account in this wallet with sufficient native tokens.

### Parameter files

Navigate to src/config, where you will find two files, `sample.executorConfig.json` and `sample.listenrConfig.json`. Replace `sample` by `live` and use these files to enter your own RPCs. The executor configuration requires only HTTP providers, and the listener configuration requires both HTTP and WebSockets.

If you have access to a Broker WebSocket connection for early execution, you may enter the corresponding URL in the listener config file.

All other values can be left unchanged.

### Docker Compose

Inspect the file `perpetuals.yml`, and edit as follows:

- The services named `redis` and `blockchain-streamer` must be left untouched.
- The services under `Executors` indicate which perpetual orders will be executed.
  - Each perpetual should use a different value for `WALLET_INDEX` for best performance.
  - If using a multi-server setup, we recommend either using different seed phrases. Note that in this case you must also specify different values for `PEER_INDEX` in the corresponding `.env` files.
- A file named `perpetuals-testnet.yml` is provided, fully populated with all the details needed to run the executor bots on testnet. You may use this as guidance when populating your own `perpetuals.yml` file.

## 3 - Starting the Executors

### Testnet

Start all executors bots on testnet by running

```
$ sudo docker compose -f perpetuals-testnet.yml up --build
```

### Mainnet

If you have already properly configured all the perpetuals you want to run in the `perpetuals.yml` file, then you can start the bots by running:

```
$ sudo docker compose -f perpetuals.yml up --build
```

## 4 - Monitoring & Maintenance

## Monitoring via Docker

### List services

Open a command terminal and run

`$ sudo docker ps`

The output should look like this:

```jsx
m66260@bots-2:~$ sudo docker ps
CONTAINER ID   IMAGE                                    COMMAND                  CREATED      STATUS             PORTS                                       NAMES
70107b314975   d8x-order-referrer-btc-usd-matic         "docker-entrypoint.s…"   2 days ago   Up About an hour                                               d8x-order-referrer-btc-usd-matic-1
fd5c32c01295   d8x-order-referrer-matic-usd-matic       "docker-entrypoint.s…"   2 days ago   Up About an hour                                               d8x-order-referrer-matic-usd-matic-1
0105351965bb   d8x-order-referrer-chf-usdc-usdc         "docker-entrypoint.s…"   2 days ago   Up About an hour                                               d8x-order-referrer-chf-usdc-usdc-1
d90fb46a981a   d8x-order-referrer-xau-usdc-usdc         "docker-entrypoint.s…"   2 days ago   Up About an hour                                               d8x-order-referrer-xau-usdc-usdc-1
6e1bdb07657b   d8x-order-referrer-gbp-usdc-usdc         "docker-entrypoint.s…"   2 days ago   Up About an hour                                               d8x-order-referrer-gbp-usdc-usdc-1
5e577996f0fc   d8x-order-referrer-eur-usdc-usdc         "docker-entrypoint.s…"   2 days ago   Up 35 hours                                                    d8x-order-referrer-eur-usdc-usdc-1
7dd88d0ec0b8   d8x-order-referrer-matic-usdc-usdc       "docker-entrypoint.s…"   2 days ago   Up About an hour                                               d8x-order-referrer-matic-usdc-usdc-1
93fb7abf9012   d8x-order-referrer-jpy-usdc-usdc         "docker-entrypoint.s…"   2 days ago   Up About an hour                                               d8x-order-referrer-jpy-usdc-usdc-1
b72e9330d447   d8x-order-referrer-btc-usdc-usdc         "docker-entrypoint.s…"   2 days ago   Up About an hour                                               d8x-order-referrer-btc-usdc-usdc-1
1762ebbebaff   d8x-order-referrer-eth-usdc-usdc         "docker-entrypoint.s…"   2 days ago   Up About an hour                                               d8x-order-referrer-eth-usdc-usdc-1
f735b0a9f9e4   d8x-order-referrer-eth-usd-matic         "docker-entrypoint.s…"   2 days ago   Up About an hour                                               d8x-order-referrer-eth-usd-matic-1
0a7afedf17d0   d8x-order-referrer-blockchain-streamer   "docker-entrypoint.s…"   2 days ago   Up 2 days                                                      d8x-order-referrer-blockchain-streamer-1
752d8e5e8eb4   redis                                    "docker-entrypoint.s…"   2 days ago   Up 2 days          0.0.0.0:6379->6379/tcp, :::6379->6379/tcp   d8x-order-referrer-redis-1
```

**Container ID**

Unique identifier for each service. You can use this to inspect in real-time the activity of each service.

**Created and Status**

- redis: this service should never stop running under normal circumstances. Expect to see a ‘status’ of several days or longer.
- blockchain-streamer: this service listens to blockchain events and broker websocket messages. Expect to see a status of a few days.
- all other services: these are the trading bots and they restart often due to e.g. RPCs failing, Pyth services not responding, insufficient wallet balance. Expect to see a status of a couple of hours or more.

The most obvious sign of a malfunction is seeing any of the status fields showing a number much lower than what is expected according the above list. 

Example: if a bot status does not last longer than a few minutes even after calling `docker ps` multiple times, this is a sign that it’s stuck performing an action that continues to fail.

### Inspect individual services

Individual services can be inspected in real-time by means of the docker command

`sudo docker logs -f <CONTAINER_ID>`

**Blockchain Streamer**

Inspecting the blockchain streamer logs allows you to see:

- On-chain events received by this server (orders created, cancelled, executed, failed, etc)
- Periodic checks on the block-time (time elapsed between new blocks received)
- Switching between Websocket and HTTP mode for event listening (e.g. when an RPC stops responding and/or block-time implied by the events is longer than the specified tolerance)
- Current event-listening mode (WS or HTTP)

**Bots**

Inspecting individual bots you will see information logged at start-up time and ongoing activity:

At startup:

- Balances: bot-treasury and executor wallets
- Configuration: perpetual Id, contract addresses, executor wallets, earnings wallet. For example:

```jsx
perpetualId: 200007,
orderBook: '0x934b1c4D0f584F2B08d895424dd78B8113c61AeB',
proxy: '0x0e23619704556945B1C7CB550Dee6D428f7d2E2B',
earnings: '0x6FE871703EB23771c4016eB62140367944e8EdFc',
servers: 1,
serverIndex: 0,
wallets: [
'0x084FB09a56538714E7C77260f01722143aB41Cc1',
'0x723dee1EA2e6973B2175feA96139E54B875a506E',
'0xc237BA31dc5F30D74B00cc963d284259e24df738'
]
```

- Orders posted, executed, failed, etc
- Periodic refresh of the entire order book

## Restarting

**Pre-requisites:**

- Check-out the main branch of the order-executor repository
- Retrieve the config files from `/home/qShared/`: `.env` , `live.executorConfig.json`, and `live.listenerConfig.json`
- Open a terminal and navigate to the root directory of the repo you checked out

**Restarting without building:**

If you believe the current configuration is correct and only want to restart the services without changing the specified parameters, you can stop and restart them as follows:

```bash
sudo docker compose -f perpetuals-testnet.yml down
sudo docker compose -f perpetuals-testnet.yml up -d
```

**Rebuilding the services**

Code changes or parameter changes require rebuilding the services before restarting.

At the time of writing, there is a known issue with storage-leak in Docker subvolumes. This means in order to properly clear all the storage you must follow a few extra steps:

```bash
sudo docker rm -f $(sudo docker ps -aq)
sudo docker rmi $(sudo docker images -aq)
sudo systemctl stop docker
sudo rm -rf /var/lib/docker
sudo systemctl start docker
```

Now that all Docker services have been stopped and removed from storage, you may re-build them:

```bash
sudo docker compose -f perpetuals-testnet.yml up
```
