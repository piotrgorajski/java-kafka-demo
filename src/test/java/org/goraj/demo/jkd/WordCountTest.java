package org.goraj.demo.jkd;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class WordCountTest {

    @Test
    public void shouldCreateTopology() {
        //given
        Properties configuration = new Properties();
        configuration.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-test");
        configuration.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "fake:1234");
        configuration.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        configuration.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        //FIXME I am getting: org.apache.kafka.streams.errors.StreamsException: java.nio.file.DirectoryNotEmptyException: \tmp\kafka-streams\word-count-test1\0_0
        //FIXME try adding StreamConfig.STATE_DIR ? https://stackoverflow.com/questions/37994217/exception-in-thread-streamthread-1-org-apache-kafka-streams-errors-streamsexce
        //FIXME is it related to: https://issues.apache.org/jira/browse/KAFKA-6647 ??

        //when
        Topology topology = new WordCount().createTopology();
        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, configuration)) {
            ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>("streams-plaintext-input", new StringSerializer(), new StringSerializer());
            topologyTestDriver.pipeInput(factory.create("words sentence with repeated words with"));

            //then
            OutputVerifier.compareKeyValue(readOutput(topologyTestDriver), "words", 1L);
            OutputVerifier.compareKeyValue(readOutput(topologyTestDriver), "sentence", 1L);
            OutputVerifier.compareKeyValue(readOutput(topologyTestDriver), "with", 1L);
            OutputVerifier.compareKeyValue(readOutput(topologyTestDriver), "repeated", 1L);
            OutputVerifier.compareKeyValue(readOutput(topologyTestDriver), "words", 2L);
            OutputVerifier.compareKeyValue(readOutput(topologyTestDriver), "with", 2L);
            assertThat(readOutput(topologyTestDriver)).isNull();
        }
    }

    private ProducerRecord<String, Long> readOutput(TopologyTestDriver topologyTestDriver) {
        return topologyTestDriver.readOutput("streams-word-count-output", new StringDeserializer(), new LongDeserializer());
    }
}