import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {
    private static final Logger logger = LogManager.getLogger();
    public static void main(final String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.STATE_DIR_CONFIG, AppConfigs.stateStoreLocation);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> KS0 = streamsBuilder.stream(AppConfigs.topicName);
        KStream<String, String> KS1= KS0.flatMapValues(v -> Arrays.asList(v.toLowerCase().split(" ")));// criando outra mensagem, por isso do flamapvalues

        KGroupedStream<String, String> KGS1 = KS1.groupBy((k, v) -> v); //por traz ele executa um KSQL  group by value
        KTable<String, Long> KT1 = KGS1.count(); // o retorno vai para um KTABLE select value, count(*) from KS1
        KT1.toStream().print(Printed.<String, Long>toSysOut().withLabel("KT1"));

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down stream");
            streams.close();
        }));
    }
}
