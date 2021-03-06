package com.course.kafka.broker.stream.feedback;

import static java.util.stream.Collectors.toList;

import java.util.Arrays;
import java.util.Set;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

import com.course.kafka.broker.message.FeedbackMessage;

//@Configuration
public class FeedbackSixStream {

	private static final Set<String> BAD_WORDS = Set.of("angry", "sad", "bad");
	private static final Set<String> GOOD_WORDS = Set.of("happy", "good", "helpful");

	@SuppressWarnings("unchecked")
	@Bean
	public KStream<String, FeedbackMessage> kstreamFeedback(StreamsBuilder builder) {
		var stringSerde = Serdes.String();
		var feedbackSerde = new JsonSerde<>(FeedbackMessage.class);

		var sourceStream = builder.stream("t.commodity.feedback", Consumed.with(stringSerde, feedbackSerde));

		var feedbackStreams = sourceStream.flatMap(splitWords()).branch(isGoodWord(), isBadWord());

		feedbackStreams[0].through("t.commodity.feedback-six-good").groupByKey().count().toStream()
				.to("t.commodity.feedback-six-good-count");
		feedbackStreams[1].through("t.commodity.feedback-six-bad").groupByKey().count().toStream()
				.to("t.commodity.feedback-six-bad-count");

		// additional requirement
		feedbackStreams[0].groupBy((key, value) -> value).count().toStream()
				.to("t.commodity.feedback-six-good-count-word");
		feedbackStreams[1].groupBy((key, value) -> value).count().toStream()
				.to("t.commodity.feedback-six-bad-count-word");

		return sourceStream;
	}

	private Predicate<? super String, ? super String> isBadWord() {
		return (key, value) -> BAD_WORDS.contains(value);
	}

	private Predicate<? super String, ? super String> isGoodWord() {
		return (key, value) -> GOOD_WORDS.contains(value);
	}

	private KeyValueMapper<String, FeedbackMessage, Iterable<KeyValue<String, String>>> splitWords() {
		return (key, value) -> Arrays
				.asList(value.getFeedback().replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+")).stream()
				.distinct().map(word -> KeyValue.pair(value.getLocation(), word)).collect(toList());
	}

}
