package guru.learningjournal.kafka.examples;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamingTableApp {

    private static Logger log = LoggerFactory.getLogger(StreamingTableApp.class);

    /*
    * Exemplo console producer HDFCBANK: 1200
     * */

    public static void main(String[] args) {
        Properties pros = new Properties();
        pros.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        pros.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        pros.put(StreamsConfig.STATE_DIR_CONFIG, AppConfigs.stateStoreLocation);
        pros.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        pros.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KTable<String, String> KTO = streamsBuilder.table(AppConfigs.topicName);
        KTO.toStream().print(Printed.<String, String>toSysOut().withLabel("KTO"));

        KTable<String, String> KT1 = KTO.filter((k, v) -> k.matches(AppConfigs.regExSymbol) && !v.isEmpty(), Materialized.as(AppConfigs.stateStoreName));
        KT1.toStream().print(Printed.<String, String>toSysOut().withLabel("KT1"));

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), pros);

        QueryServer server = new QueryServer(streams, AppConfigs.queryServerHost, AppConfigs.queryServerPort);
        streams.setStateListener((newState, oldState) -> {
            log.info("State changing to " + newState + " from " + oldState);
            server.setActive(newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING);
        });

        streams.start();
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down servers");
            streams.close();
            server.stop();
        }));

    }
}
