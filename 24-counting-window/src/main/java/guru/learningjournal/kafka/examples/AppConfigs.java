package guru.learningjournal.kafka.examples;

class AppConfigs {

    final static String applicationID = "CountingWindowApp";
    final static String bootstrapServers = "localhost:9092,localhost:9093,localhost:9094";
    final static String posTopicName = "simple-invoice3";
    final static String stateStoreName = "tmp/state-store";
}
