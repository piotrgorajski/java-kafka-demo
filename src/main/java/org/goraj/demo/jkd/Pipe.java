package org.goraj.demo.jkd;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Slf4j
public final class Pipe {

    public static void main(String[] args) {
        Properties configuration = new Properties();
        configuration.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        configuration.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configuration.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        configuration.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.stream("streams-plaintext-input").to("streams-plaintext-output");

        Topology topology = streamsBuilder.build();

        final KafkaStreams streams = new KafkaStreams(topology, configuration);
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (InterruptedException e) {
            log.error("Running kafka streams has been interrupted", e);
        }
    }
}
