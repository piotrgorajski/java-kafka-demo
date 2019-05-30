package org.goraj.demo.jkd;

import org.apache.kafka.clients.producer.ProducerRecord;
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

public class LineSplitTest {

    @Test
    public void shouldCreateTopology() {
        //given
        Properties configuration = new Properties();
        configuration.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "line-split-test");
        configuration.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "fake:1234");
        configuration.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        configuration.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        //when
        Topology topology = new LineSplit().createTopology();
        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, configuration)) {
            ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>("streams-plaintext-input", new StringSerializer(), new StringSerializer());
            topologyTestDriver.pipeInput(factory.create("this is value"));

            //then
            OutputVerifier.compareValue(readOutput(topologyTestDriver), "this");
            OutputVerifier.compareValue(readOutput(topologyTestDriver), "is");
            OutputVerifier.compareValue(readOutput(topologyTestDriver), "value");
            assertThat(readOutput(topologyTestDriver)).isNull();
        }
    }

    private ProducerRecord<String, String> readOutput(TopologyTestDriver topologyTestDriver) {
        return topologyTestDriver.readOutput("streams-line-split-output", new StringDeserializer(), new StringDeserializer());
    }
}