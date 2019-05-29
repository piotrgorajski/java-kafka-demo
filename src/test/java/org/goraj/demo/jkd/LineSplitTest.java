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

public class LineSplitTest {

    @Test
    public void shouldCreateTopology() {
        //given
        Properties configuration = new Properties();
        configuration.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "line-split-test");
        configuration.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "fake:1234");
        Topology topology = new LineSplit().createTopology();
        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, configuration)) {
            //when
            ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>("streams-plaintext-input", new StringSerializer(), new StringSerializer());
            // FIXME: It does not work on java 11 due to:
            // java.lang.ClassCastException: class [B cannot be cast to class java.lang.String ([B and java.lang.String are in module java.base of loader 'bootstrap')
            topologyTestDriver.pipeInput(factory.create("this is value"));

            //then
            ProducerRecord<String, String> outputRecord = topologyTestDriver.readOutput("streams-line-split-output", new StringDeserializer(), new StringDeserializer());
            OutputVerifier.compareValue(outputRecord, "this is value");
        }
    }
}