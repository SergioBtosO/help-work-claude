package com.example.kafka.consumer;

import com.example.kafka.service.MessageService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = "${kafka.topic.name}")
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class KafkaConsumerTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Value("${kafka.topic.name}")
    private String topic;

    private BlockingQueue<ConsumerRecord<String, String>> records;
    private KafkaMessageListenerContainer<String, String> container;
    private Producer<String, String> producer;

    @MockBean
    private MessageService messageService;

    @BeforeAll
    void setUp() {
        // Configurar el consumidor de prueba
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-group", "true", embeddedKafkaBroker);
        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(
                consumerProps, new StringDeserializer(), new StringDeserializer());

        ContainerProperties containerProperties = new ContainerProperties(topic);
        container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
        records = new LinkedBlockingQueue<>();
        container.setupMessageListener((MessageListener<String, String>) records::add);
        container.start();

        // Esperar a que el contenedor esté listo
        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());

        // Configurar el productor de prueba
        Map<String, Object> producerProps = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
        producer = new DefaultKafkaProducerFactory<>(producerProps, new StringSerializer(), new StringSerializer()).createProducer();
    }

    @AfterAll
    void tearDown() {
        if (container != null) {
            container.stop();
        }
        if (producer != null) {
            producer.close();
        }
    }

    @Test
    void testKafkaConsumer() throws Exception {
        // Given
        String message = "Mensaje de prueba";
        when(messageService.processMessage(anyString())).thenReturn(true);

        // When
        producer.send(new ProducerRecord<>(topic, message)).get();

        // Then
        ConsumerRecord<String, String> received = records.poll(10, TimeUnit.SECONDS);
        assertThat(received).isNotNull();
        assertThat(received.value()).isEqualTo(message);

        // Verificar que el messageService fue llamado
        verify(messageService, times(1)).processMessage(message);
    }

    @Test
    void testKafkaConsumerErrorHandling() throws Exception {
        // Given
        String message = "Mensaje de error";
        when(messageService.processMessage(message)).thenReturn(false);

        // When
        producer.send(new ProducerRecord<>(topic, message)).get();

        // Then
        ConsumerRecord<String, String> received = records.poll(10, TimeUnit.SECONDS);
        assertThat(received).isNotNull();
        assertThat(received.value()).isEqualTo(message);

        // Verificar que el messageService fue llamado
        verify(messageService, times(1)).processMessage(message);
    }
}
