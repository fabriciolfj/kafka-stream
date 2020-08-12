package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class RewardPartitioner implements StreamPartitioner<String, PosInvoice> {

    @Override
    public Integer partition(String topic, String key, PosInvoice value, int numPartitions) {
        int result = value.getCustomerCardNo().hashCode() % numPartitions;

        if (result < 0) {
            result = result * -1;
        }

        return result;
    }
}
