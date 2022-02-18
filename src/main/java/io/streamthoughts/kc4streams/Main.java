package io.streamthoughts.kc4streams;

import io.streamthoughts.kc4streams.error.Failed;
import io.streamthoughts.kc4streams.error.GlobalDeadLetterTopicCollector;
import io.streamthoughts.kc4streams.error.GlobalDeadLetterTopicCollectorConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.junit.platform.commons.function.Try;

import java.util.HashMap;
import java.util.Map;

public class Main {

    public static final String INPUT_TOPIC = "input-topic";

    public static void main(String[] args) {

        // Create KafkaStreams client configuration
        Map<String, Object> streamsConfigs = new HashMap<>();

        // Initialize the GlobalDeadLetterTopicCollector.
        GlobalDeadLetterTopicCollector.getOrCreate(streamsConfigs);

        // Create a Kafka Stream Topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> stream = streamsBuilder.stream(INPUT_TOPIC);
        stream.mapValues((key, value) -> {
            Long output = null;
            try {
                output = Long.parseLong(value);
            } catch (Exception e) {
                // Sends the corrupted-record to a DLQ
                GlobalDeadLetterTopicCollector.get().send(
                        INPUT_TOPIC + "-DLQ",
                        key,
                        value,
                        Serdes.String().serializer(),
                        Serdes.String().serializer(),
                        Failed.withProcessingError((String) streamsConfigs.get(StreamsConfig.APPLICATION_ID_CONFIG), e)
                );
            }
            return output;
        });
    }
}