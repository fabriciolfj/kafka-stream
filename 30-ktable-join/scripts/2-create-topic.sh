kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 2 --topic user-master & kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 2 --topic user-login