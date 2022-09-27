# go-queue-kafka

A proof of concept for a in-memory/persistent queue that uses kafka (incredibly similar to durostore)

## Commands

To create a topic

```sh
kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 2 --topic queue_in
```

To alter an existing topic

```sh
kafka-topics.sh --zookeeper zookeeper:2181 --alter --topic queue_in -partitions 2
```

## Links

- [https://medium.com/@ronnansouza/setting-up-a-kafka-broker-using-docker-creating-a-producer-and-consumer-group-with-multiple-384b724cd324](https://medium.com/@ronnansouza/setting-up-a-kafka-broker-using-docker-creating-a-producer-and-consumer-group-with-multiple-384b724cd324)
- [https://dattell.com/data-architecture-blog/load-balancing-with-kafka/](https://dattell.com/data-architecture-blog/load-balancing-with-kafka/)
- [https://hevodata.com/learn/kafka-partitions/](https://hevodata.com/learn/kafka-partitions/)
- [https://github.com/Shopify/sarama/tree/main/examples](https://github.com/Shopify/sarama/tree/main/examples)
