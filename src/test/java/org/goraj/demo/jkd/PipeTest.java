package org.goraj.demo.jkd;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.Test;

import java.util.Properties;

public class PipeTest {

    @Test
    public void shouldCreateTopology() {
        //given
        Properties configuration = new Properties();
        configuration.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "pipe-test");
        configuration.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "fake:1234");
        Topology topology = new Pipe().createTopology();
        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, configuration)) {
            //when
            ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>("streams-plaintext-input", new StringSerializer(), new StringSerializer());
            String inputValue = "this is value";
            topologyTestDriver.pipeInput(factory.create(inputValue));

            //then
            ProducerRecord<String, String> outputRecord = topologyTestDriver.readOutput("streams-plaintext-output", new StringDeserializer(), new StringDeserializer());
            OutputVerifier.compareValue(outputRecord, inputValue);
        }
    }
}