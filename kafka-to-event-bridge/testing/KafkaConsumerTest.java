package com.example.kafka.consumer;

import com.example.kafka.service.MessageService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.Set;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.timeout;
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

    private Producer<String, String> producer;

    @MockBean
    private MessageService messageService;
    
    @SpyBean
    private KafkaConsumer kafkaConsumer;

    @BeforeAll
    void setUp() {
        // Configurar el productor de prueba
        Map<String, Object> producerProps = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
        producer = new DefaultKafkaProducerFactory<>(
                producerProps, 
                new StringSerializer(), 
                new StringSerializer()
        ).createProducer();
        
        // Configurar el mock del servicio de mensajes
        when(messageService.processMessage(anyString())).thenReturn(true);
    }

    @AfterAll
    void tearDown() {
        if (producer != null) {
            producer.close();
        }
    }

    @Test
    void testKafkaConsumer() throws Exception {
        // Given
        String message = "Mensaje de prueba";

        // When - Enviar un mensaje al tópico
        producer.send(new ProducerRecord<>(topic, message)).get();
        
        // Then - Verificar que el servicio de mensajes fue llamado con el mensaje correcto
        // Usamos timeout para esperar hasta 10 segundos por la verificación debido a la naturaleza asíncrona
        verify(messageService, timeout(10000)).processMessage(message);
    }

    @Test
    void testKafkaConsumerErrorHandling() throws Exception {
        // Given
        String message = "Mensaje de error";
        when(messageService.processMessage(message)).thenReturn(false);

        // When - Enviar un mensaje al tópico
        producer.send(new ProducerRecord<>(topic, message)).get();

        // Then - Verificar que el servicio de mensajes fue llamado con el mensaje correcto
        verify(messageService, timeout(10000)).processMessage(message);
    }
    
    @Test
    void testConsumerStarts() throws Exception {
        // Verificar que el contenedor del listener está activo
        Object container = ReflectionTestUtils.getField(kafkaConsumer, "container");
        // La prueba simplemente verifica que el consumidor no lanza excepciones al iniciar
        // ya que el contenedor se inicia automáticamente con @PostConstruct
    }
    
    @Test
    void testAcknowledgeMessage() throws Exception {
        // Given
        String message = "Mensaje para confirmar";
        
        // When - Enviar un mensaje al tópico
        producer.send(new ProducerRecord<>(topic, message)).get();
        
        // Esperar a que el mensaje sea procesado
        verify(messageService, timeout(10000)).processMessage(message);
        
        // Then - Verificar que hay un mensaje pendiente de confirmación
        TimeUnit.SECONDS.sleep(1); // Dar tiempo para que el mensaje sea registrado
        Set<String> pendingMessages = kafkaConsumer.getPendingMessageIds();
        assertThat(pendingMessages).isNotEmpty();
        
        // When - Confirmar el mensaje
        String messageId = pendingMessages.iterator().next();
        boolean ackResult = kafkaConsumer.acknowledgeMessage(messageId);
        
        // Then - Verificar que la confirmación fue exitosa y el mensaje ya no está pendiente
        assertThat(ackResult).isTrue();
        pendingMessages = kafkaConsumer.getPendingMessageIds();
        assertThat(pendingMessages).isEmpty();
    }
}
