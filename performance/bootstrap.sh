#!/usr/bin/env bash
set -euo pipefail
umask 022
cd /tmp/payment-perf
mkdir -p sources bin results
test -d sources/node-current/.git || git clone /tmp/payment-perf/node.bundle sources/node-current
git -C sources/node-current checkout --detach e2b331958a5704016310153a7d33b7a634758f38
test -d sources/node-pr1/.git || git clone /tmp/payment-perf/node.bundle sources/node-pr1
git -C sources/node-pr1 checkout --detach bbe126e5eddbf59f9cc9f94d3c37703215c26850
test -d sources/node-combined/.git || git clone /tmp/payment-perf/node.bundle sources/node-combined
test -d sources/go-livepeer/.git || git clone --filter=blob:none --no-checkout https://github.com/livepeer/go-livepeer.git sources/go-livepeer
git -C sources/go-livepeer fetch --depth=1 origin 2a4f7c48239a3b3e6ce49ca793a41253ec664b26
git -C sources/go-livepeer checkout --detach FETCH_HEAD
test -d sources/protocol/.git || git clone --filter=blob:none --no-checkout https://github.com/livepeer/protocol.git sources/protocol
git -C sources/protocol fetch --depth=1 origin 3c01f3a3e8c494ea2f89b77d03eb0a68a4e15518
git -C sources/protocol checkout --detach FETCH_HEAD
docker build -t payment-perf-base -f scope/docker/Dockerfile scope/docker
docker build -t payment-perf-runtime -f scope/performance/Dockerfile scope/performance
docker pull ghcr.io/foundry-rs/foundry:v1.9.0
docker run --rm --network host -e XDG_DATA_HOME=/tmp/hardhat-data -e XDG_CONFIG_HOME=/tmp/hardhat-config -e XDG_CACHE_HOME=/tmp/hardhat-cache -e HARDHAT_DISABLE_TELEMETRY_PROMPT=true -v /tmp/payment-perf:/work -w /work/sources/protocol node:16-bullseye \
  bash -c 'git config --global --add safe.directory /work/sources/protocol && yarn install --frozen-lockfile && ./node_modules/.bin/hardhat typechain'
docker inspect payment-perf-chain >/dev/null 2>&1 || docker run -d --name payment-perf-chain --network host --entrypoint anvil \
  ghcr.io/foundry-rs/foundry:v1.9.0 --host 127.0.0.1 --chain-id 54321 --block-time 1 --slots-in-an-epoch 1 --gas-limit 30000000
docker run --rm --network host -e XDG_DATA_HOME=/tmp/hardhat-data -e XDG_CONFIG_HOME=/tmp/hardhat-config -e XDG_CACHE_HOME=/tmp/hardhat-cache -e HARDHAT_DISABLE_TELEMETRY_PROMPT=true -v /tmp/payment-perf:/work -w /work/sources/protocol node:16-bullseye \
  bash -c 'git config --global --add safe.directory /work/sources/protocol && ./node_modules/.bin/hardhat deploy --tags Contracts,Poll --network gethDev && ./node_modules/.bin/hardhat unpause --network gethDev && ./node_modules/.bin/hardhat deploy --tags ARBITRUM_LPT_DUMMIES --network gethDev'
docker inspect payment-perf-rounds >/dev/null 2>&1 || docker run -d --name payment-perf-rounds --network host -v /tmp/payment-perf:/work \
  -e NODE_PATH=/work/sources/protocol/node_modules node:16-bullseye node /work/scope/performance/rounds.cjs
docker volume create payment-perf-modules
docker volume create payment-perf-build
docker run --rm --network host -v /tmp/payment-perf:/work \
  -v payment-perf-modules:/go/pkg/mod -v payment-perf-build:/root/.cache/go-build \
  payment-perf-runtime bash /work/scope/performance/build.sh
