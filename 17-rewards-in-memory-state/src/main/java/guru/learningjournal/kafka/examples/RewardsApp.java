package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.AppSerdes;
import guru.learningjournal.kafka.examples.types.Notification;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class RewardsApp {

    private static final Logger logger = LoggerFactory.getLogger(RewardsApp.class);

    public static void main(String[] args) {
        Properties pros = new Properties();
        pros.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        pros.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, PosInvoice> KS0 = builder.stream(
                AppConfigs.posTopicName,
                Consumed.with(AppSerdes.String(), AppSerdes.PosInvoice())
        ).filter((key, value) -> value.getCustomerType().equalsIgnoreCase(AppConfigs.CUSTOMER_TYPE_PRIME));

        StoreBuilder kvStoreBuilder = Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(AppConfigs.REWARDS_STORE_NAME), AppSerdes.String(), AppSerdes.Double());
        builder.addStateStore(kvStoreBuilder); // cria um banco Roq em in memory

        /*
        * O particionamento é uma atividade cara,
            e impacta o desempenho de seu aplicativo de streaming.
            Assim,
            Você deve projetar sua chave de mensagem para minimizar a necessidade de reparticionamento
            e evite-o sempre que puder.
            Contudo,
            a necessidade de repartição é inevitável para qualquer estrutura de processamento distribuída,
            e Kafka Streams não é diferente.
            Você também deve se certificar de criar o tópico intermediário com antecedência
            e configurar o número de partições e o período de retenção de forma adequada.
        * */
        KS0.through(AppConfigs.REWARDS_TEMP_TOPIC, Produced.with(AppSerdes.String(), AppSerdes.PosInvoice(), new RewardPartitioner()))
                /*estratégia de nao reparticionamento acaba aqui*/
                .transformValues(() -> new RewardsTransformer(), AppConfigs.REWARDS_STORE_NAME)
                .to(AppConfigs.notificationTopic,
                        Produced.with(AppSerdes.String(), AppSerdes.Notification()));

        KafkaStreams streams = new KafkaStreams(builder.build(), pros);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping streams");
            streams.cleanUp();
        }));


    }
}
