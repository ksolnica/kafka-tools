Kafka Tools
===

## Test environment 

Set up test Kafka cluster

```bash
# Create
docker-compose up -d

# Scale up
docker-compose scale kafka=3

# Destroy
docker-compose down

# Prune unused volumes
docker volume prune -f
```

## Select a new partition leader

- Shifts replicas for selected partitions by one and elects a new leader

```bash
java ks.tools.kafka.admin.KafkaLeaderChange ZK_HOST:PORT KAFKA_HOST:PORT TOPIC:PARTITION
```

Example:
```bash
java ks.tools.kafka.admin.KafkaLeaderChange :2181 :9092 topic:0 topic:2
```
