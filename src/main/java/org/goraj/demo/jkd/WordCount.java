package org.goraj.demo.jkd;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Slf4j
public final class WordCount {

    public static void main(String[] args) {
        KafkaStreams streams = new KafkaStreams(createTopology(), prepareConfiguration());
        runStreamsClient(streams);
    }

    private static Properties prepareConfiguration() {
        Properties configuration = new Properties();
        configuration.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-word-count");
        configuration.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configuration.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        configuration.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return configuration;
    }

    private static Topology createTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> source = streamsBuilder.stream("streams-plaintext-input");
        KStream<String, String> words = source.flatMapValues(value ->
                Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+"))
        );
        KTable<String, Long> counts = words.groupBy((key, value) -> value).count(Materialized.as("counts-store"));
        counts.toStream().to("streams-word-count-output", Produced.with(Serdes.String(), Serdes.Long()));
        return streamsBuilder.build();
    }

    private static void runStreamsClient(KafkaStreams streams) {
        CountDownLatch latch = addShutdownHookToCloseStreams(streams);
        try {
            streams.start();
            latch.await();
        } catch (InterruptedException e) {
            log.error("Running kafka streams has been interrupted", e);
        }
    }

    private static CountDownLatch addShutdownHookToCloseStreams(final KafkaStreams streams) {
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });
        return latch;
    }
}
