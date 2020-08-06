package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.AppSerdes;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class PosFanoutApp {

    private final static Logger LOGGER = LoggerFactory.getLogger(PosFanoutApp.class);

    /*
    topology seria um fluxo, para ontem minha mensagem vai, conforme filtros do meu kafkastream no topic consumido
    * colocando o stream em um topology, posso escalar o consumo, passando o numero de threads que necessito.
    * */
    public static void main(String[] args) {
        Properties pros = new Properties();
        pros.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        pros.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        pros.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 9);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, PosInvoice> KS0 = builder.stream(AppConfigs.posTopicName, Consumed.with(AppSerdes.String(), AppSerdes.PosInvoice()));

        KS0.filter((k ,v) -> v.getDeliveryType().equalsIgnoreCase(AppConfigs.DELIVERY_TYPE_HOME_DELIVERY))
                .to(AppConfigs.shipmentTopicName, Produced.with(AppSerdes.String(), AppSerdes.PosInvoice()));

        KS0.filter((k, v) -> v.getCustomerType().equalsIgnoreCase(AppConfigs.CUSTOMER_TYPE_PRIME))
                .mapValues(invoice -> RecordBuilder.getNotification(invoice))
                .to(AppConfigs.notificationTopic, Produced.with(AppSerdes.String(), AppSerdes.Notification()));

        KS0.mapValues(invoice -> RecordBuilder.getMaskedInvoice(invoice))
                .flatMapValues(invoice -> RecordBuilder.getHadoopRecords(invoice))
                .to(AppConfigs.hadoopTopic, Produced.with(AppSerdes.String(), AppSerdes.HadoopRecord()));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, pros);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Stop stream");
            streams.close();
        }));
    }
}
