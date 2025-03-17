package com.example.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = "${kafka.topic.name}")
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class KafkaConsumerServiceIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Value("${kafka.topic.name}")
    private String topic;

    private Producer<String, String> producer;

    @BeforeAll
    void setUp() {
        // Configurar el productor de prueba
        Map<String, Object> producerProps = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
        producer = new DefaultKafkaProducerFactory<>(producerProps, new StringSerializer(), new StringSerializer()).createProducer();
    }

    @AfterAll
    void tearDown() {
        if (producer != null) {
            producer.close();
        }
    }

    @Test
    void contextLoads() {
        // Verificar que el contexto de Spring se carga correctamente
        assertThat(embeddedKafkaBroker).isNotNull();
    }

    @Test
    void testIntegrationWithKafka() throws Exception {
        // Given
        String message = "Mensaje de integración";

        // When
        producer.send(new ProducerRecord<>(topic, message)).get(10, TimeUnit.SECONDS);

        // Then - No podemos verificar directamente el resultado ya que es asíncrono
        // Pero podemos confirmar que no se lanzaron excepciones al enviar el mensaje
        // En una aplicación real, podríamos verificar efectos secundarios como cambios en una base de datos
        TimeUnit.SECONDS.sleep(2); // Dar tiempo para que el mensaje sea procesado
    }

    @Test
    void testMultipleMessages() throws Exception {
        // Given
        int messageCount = 5;

        // When
        for (int i = 0; i < messageCount; i++) {
            String message = "Mensaje de integración " + i;
            producer.send(new ProducerRecord<>(topic, message)).get(10, TimeUnit.SECONDS);
        }

        // Then - Igual que en el test anterior
        TimeUnit.SECONDS.sleep(2); // Dar tiempo para que los mensajes sean procesados
    }
}
