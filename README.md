# Platform
XRPL Deep search platform is a data processing pipeline to store XRPL transactions in a distributed search and analytics platform. The platform enables simple data retrieval, aggregate information and discovering trends and patterns in XRPL. 

### What is Deep search?
XRP Ledger exploration tools and APIs available today, such as rippled, clio and various explorer APIs provide access to ledger data based on object's primary key such as ledger index, transaction hash, account address, NFT id, object ids etc. This project aims to provide deeper search capability such as filtering transactions by source/destination tags, range query over payment amounts, aggregate volumes and much more. This is enabled by indexing all properties of the transactions in an analytics engine.

### Requirements

1. [Apache Kafka](https://kafka.apache.org)
2. [rippled](https://xrpl.org/install-rippled.html)
3. Access to full history rippled (if backfilling older ledgers)
4. ClickHouse (see docker-compose)

### Architecture

![Search platform architecture](https://github.com/xrpscan/platform/blob/main/assets/xrpscan-platform.png?raw=true)

### Installation

1. This project is known to run on Linux and macOS. This README lists out steps to
run the service on CentOS.

2. Install Docker via this guide: https://docs.docker.com/engine/install/centos/

3. Configure Docker to run as non-root

```
usermod -aG docker non-root-user
systemctl restart docker
```

4. Install Zookeeper and Kafka

```
docker compose up -d
```

5. Install Go via this guide: https://go.dev/doc/install

6. Build deep search platform

```
dnf install make
git clone git@github.com:xrpscan/platform.git
cd platform
make
```

7. Create environment file and update settings within

```
cp .env.example .env
```

8. Start ClickHouse and dependencies via docker-compose

9. Create Kafka topics

```
docker exec kafka-broker1 kafka-topics --bootstrap-server kafka-broker1:9092 --create --if-not-exists --topic xrpl-platform-ledgers
docker exec kafka-broker1 kafka-topics --bootstrap-server kafka-broker1:9092 --create --if-not-exists --topic xrpl-platform-transactions
docker exec kafka-broker1 kafka-topics --bootstrap-server kafka-broker1:9092 --create --if-not-exists --topic xrpl-platform-transactions-processed
docker exec kafka-broker1 kafka-topics --bootstrap-server kafka-broker1:9092 --create --if-not-exists --topic xrpl-platform-validations
docker exec kafka-broker1 kafka-topics --bootstrap-server kafka-broker1:9092 --create --if-not-exists --topic xrpl-platform-manifests
docker exec kafka-broker1 kafka-topics --bootstrap-server kafka-broker1:9092 --create --if-not-exists --topic xrpl-platform-peerstatus
docker exec kafka-broker1 kafka-topics --bootstrap-server kafka-broker1:9092 --create --if-not-exists --topic xrpl-platform-consensus
docker exec kafka-broker1 kafka-topics --bootstrap-server kafka-broker1:9092 --create --if-not-exists --topic xrpl-platform-server
docker exec kafka-broker1 kafka-topics --bootstrap-server kafka-broker1:9092 --create --if-not-exists --topic xrpl-platform-default
docker exec kafka-broker1 kafka-topics --bootstrap-server kafka-broker1:9092 --create --if-not-exists --topic xrpl-platform-tx
```

10. ClickHouse DDL is auto-applied by docker-compose `clickhouse-init` service

### Running the service

1. Index new ledgers

```
./bin/platform-server
```

2. Backfill old ledgers

```
./bin/platform-cli backfill -verbose -from 82000000 -to 82999999
```

### Monitoring the service
Use `kafka-ui` (included in docker-compose) to inspect topics and messages.

### Querying data
Data is written only to ClickHouse tables. Query via HTTP:

```
curl 'http://localhost:8123/?query=SELECT%20count()%20FROM%20xrpl.tx'
```

### Maintenance
Over time, XRPL protocol may receive updates via the [amendment](https://xrpscan.com/amendments) process. Transformer logic in `indexer/modifier.go` normalizes key fields.

### References
[Ledger Stream - xrpl.org](https://xrpl.org/subscribe.html#ledger-stream)

### Developer notes
#### Updating Models
When a new amendment adds or removes fields from transaction object, review the following files and update as necessary:

* `models/transaction.go`
* `config/mapping/transaction.go`
* `models/currency.go` (for Currency fields)

### Known issues

- [xrpl-go tries to read from websocket even after its connection is closed](https://github.com/xrpscan/platform/issues/36)

### Reporting bugs
Please create a new issue in [Platform issue tracker](https://github.com/xrpscan/platform/issues)

### EOF

docker-compose down -v 
docker-compose up -d

docker exec kafka-broker1 kafka-topics --bootstrap-server kafka-broker1:9092 --create --if-not-exists --topic xrpl-platform-transactions
docker exec kafka-broker1 kafka-topics --bootstrap-server kafka-broker1:9092 --create --if-not-exists --topic xrpl-platform-ch-transactions
docker exec kafka-broker1 kafka-topics --bootstrap-server kafka-broker1:9092 --create --if-not-exists --topic xrpl-platform-ch-accounts
docker exec kafka-broker1 kafka-topics --bootstrap-server kafka-broker1:9092 --create --if-not-exists --topic xrpl-platform-ch-assets
docker exec kafka-broker1 kafka-topics --bootstrap-server kafka-broker1:9092 --create --if-not-exists --topic xrpl-platform-ch-moneyflows


Windows
go build -o .\bin\platform-server.exe .
go build -o .\bin\platform-cli.exe .\cmd\cli
go build -o .\bin\platform-orchestrator.exe .\cmd\orchestrator

run
.\bin\platform-server.exe
.\bin\platform-cli.exe


Linux
go build -o ./bin/platform-server ./
go build -o ./bin/platform-cli ./cmd/cli
go build -o ./bin/platform-orchestrator ./cmd/orchestrator

run
./bin/platform-server
./bin/platform-cli

background backfill
nohup ./run.sh > logs/backfill.log 2>&1 &

./bin/platform-orchestrator --workers 2 --from 98900000 --to 99119667 --servers "wss://s1.ripple.com/,wss://s2.ripple.com/" --check-interval 30s --verbose --redistribute-threshold 5000 > logs/orchestrator.log 2>&1 &

Остановить оркестратор touch stop.orchestrator