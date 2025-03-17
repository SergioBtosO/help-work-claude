package com.example.kafka.consumer;

import com.example.kafka.service.MessageService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@SpringBootTest
@DirtiesContext
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = "${kafka.topic.name}")
public class KafkaConsumerTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @MockBean
    private MessageService messageService;

    private Producer<String, String> producer;
    private KafkaConsumer kafkaConsumer;
    
    @Autowired
    private KafkaListenerContainerFactory<KafkaMessageListenerContainer<String, String>> kafkaListenerContainerFactoryForService;

    @BeforeEach
    void setUp() {
        // Configurar el productor de prueba
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producer = new DefaultKafkaProducerFactory<>(
                producerProps, 
                new StringSerializer(), 
                new StringSerializer()
        ).createProducer();
        
        // Configurar mock para que el procesamiento sea exitoso
        when(messageService.processMessage(anyString())).thenReturn(true);
        
        // Crear una instancia real de KafkaConsumer
        kafkaConsumer = new KafkaConsumer(messageService, kafkaListenerContainerFactoryForService);
        kafkaConsumer.start(); // Iniciar manualmente
    }

    @AfterEach
    void tearDown() {
        if (producer != null) {
            producer.close();
        }
        if (kafkaConsumer != null) {
            kafkaConsumer.stop();
        }
    }

    @Test
    void testManualAcknowledgement() throws Exception {
        // Simular un método handleMessage directo para probar la funcionalidad de acknowledgment
        ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0, "key", "test-message");
        Acknowledgment mockAck = mock(Acknowledgment.class);
        
        // Crear un CountDownLatch para esperar a que se llame a acknowledge
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(mockAck).acknowledge();
        
        // Simular el método privado handleMessage
        KafkaConsumer.AcknowledgingMessageListenerAdapter listener = 
                kafkaConsumer.new AcknowledgingMessageListenerAdapter(
                        (rec, ack) -> kafkaConsumer.handleMessage(rec, ack));
        
        // Cuando
        listener.onMessage(record, mockAck);
        
        // Verificar que el messageService fue llamado
        verify(messageService).processMessage("test-message");
        
        // Verificar que el mensaje fue confirmado (porque messageService devuelve true)
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        verify(mockAck).acknowledge();
    }
    
    @Test
    void testGetPendingMessageIds() throws Exception {
        // Simular mensajes pendientes
        ConsumerRecord<String, String> record1 = new ConsumerRecord<>("test-topic", 0, 1, "key1", "message1");
        ConsumerRecord<String, String> record2 = new ConsumerRecord<>("test-topic", 0, 2, "key2", "message2");
        
        Acknowledgment mockAck1 = mock(Acknowledgment.class);
        Acknowledgment mockAck2 = mock(Acknowledgment.class);
        
        // Configurar messageService para no procesar automáticamente
        when(messageService.processMessage(anyString())).thenReturn(false);
        
        // Agregar manualmente mensajes pendientes
        kafkaConsumer.addPendingAcknowledgment("test-topic-0-1", mockAck1);
        kafkaConsumer.addPendingAcknowledgment("test-topic-0-2", mockAck2);
        
        // Verificar que se obtienen los IDs correctos
        Set<String> pendingIds = kafkaConsumer.getPendingMessageIds();
        assertThat(pendingIds).hasSize(2);
        assertThat(pendingIds).contains("test-topic-0-1", "test-topic-0-2");
    }
    
    @Test
    void testAcknowledgeMessage() throws Exception {
        // Simular un mensaje pendiente
        Acknowledgment mockAck = mock(Acknowledgment.class);
        String messageId = "test-topic-0-3";
        
        // Agregar manualmente un mensaje pendiente
        kafkaConsumer.addPendingAcknowledgment(messageId, mockAck);
        
        // Verificar que está pendiente
        assertThat(kafkaConsumer.getPendingMessageIds()).contains(messageId);
        
        // Confirmar el mensaje
        boolean result = kafkaConsumer.acknowledgeMessage(messageId);
        
        // Verificar resultado
        assertThat(result).isTrue();
        verify(mockAck).acknowledge();
        
        // Verificar que ya no está pendiente
        assertThat(kafkaConsumer.getPendingMessageIds()).doesNotContain(messageId);
    }
}
