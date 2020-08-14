package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.types.SimpleInvoice;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Instant;

public class InvoiceTimeExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long prevTime) {
        SimpleInvoice invoice = (SimpleInvoice) consumerRecord.value();
        long eventime = Instant.parse(invoice.getCreatedTime()).toEpochMilli();
        return eventime > 0 ? eventime : prevTime;
    }
}
