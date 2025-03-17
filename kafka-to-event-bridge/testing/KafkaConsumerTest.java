package com.example.kafka.consumer;

import com.example.kafka.service.OrchestratorService;
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
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = "${kafka.topic.name}")
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class KafkaServiceTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Value("${kafka.topic.name}")
    private String topic;

    private Producer<String, String> producer;

    @MockBean
    private OrchestratorService orchestratorService;
    
    @SpyBean
    private KafkaService kafkaService;

    @BeforeAll
    void setUp() {
        // Configurar el productor de prueba
        Map<String, Object> producerProps = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
        producer = new DefaultKafkaProducerFactory<>(
                producerProps, 
                new StringSerializer(), 
                new StringSerializer()
        ).createProducer();
        
        // Configurar el mock del servicio de orquestación
        when(orchestratorService.processMessage(anyString())).thenReturn(true);
    }

    @AfterAll
    void tearDown() {
        if (producer != null) {
            producer.close();
        }
    }

    /**
     * Mensaje de ejemplo basado en la estructura de la imagen
     */
    private String createSampleMessage() {
        return "{" +
            "\"Timestamp\":\"2025-03-07T07:01:12.167+00:00\"," +
            "\"Topic\":\"SBNA.00002517.MIP_INS_HIST_E3.MODIFY.AVRO\"," +
            "\"Partition\":1," +
            "\"Offset\":39398," +
            "\"SchemaId\":null," +
            "\"SchemaType\":null," +
            "\"Key\":[0,0,0,2,96,48,51,49,55,95,48,48,57,57,51,49,57,95,48,48,46,8,0,0,0,14,102,118,100,97,116,97,46,99,114,101,97,116,101]," +
            "\"Headers\":null," +
            "\"Message\":\"\\u0011\\u00161Forge.MIP_INS_HIST_E\\u0014\\u0025\\u0015737df\\u0017\\u0014\\u001f\\u00006\\u0018\\u0004cuso\\u001e\\u001925-03-07 07:11:96700\\u00008\\u0014\\u00000\\u0014\\u0005ad\\u0014\\u0014\\u0005\u0006\"" +
        "}";
    }

    @Test
    void testKafkaServiceMessageProcessing() throws Exception {
        // Given
        String message = createSampleMessage();

        // When - Enviar un mensaje al tópico
        producer.send(new ProducerRecord<>(topic, message)).get();
        
        // Then - Verificar que el servicio de orquestación fue llamado con el mensaje correcto
        verify(orchestratorService, timeout(10000)).processMessage(message);
    }

    @Test
    void testMultipleMessageProcessing() throws Exception {
        // Given
        int messageCount = 5;

        // When - Enviar múltiples mensajes al tópico
        for (int i = 0; i < messageCount; i++) {
            String message = createSampleMessage();
            producer.send(new ProducerRecord<>(topic, message)).get();
        }
        
        // Then - Verificar que cada mensaje fue procesado
        for (int i = 0; i < messageCount; i++) {
            verify(orchestratorService, timeout(10000)).processMessage(createSampleMessage());
        }
    }

    @Test
    void testKafkaServiceStarts() {
        // Verificar que el servicio de Kafka se inicia sin errores
        assertThat(kafkaService).isNotNull();
    }

    @Test
    void testAcknowledgmentCalledOnSuccessfulProcessing() throws Exception {
        // Given
        String message = createSampleMessage();
        
        // Crear un mock de Acknowledgment
        Acknowledgment mockAcknowledgment = mock(Acknowledgment.class);
        
        // Configurar procesamiento exitoso
        when(orchestratorService.processMessage(message)).thenReturn(true);
        
        // When - Invocar manualmente el método handleMessage
        Method handleMessageMethod = ReflectionTestUtils.findMethod(
            KafkaService.class, 
            "handleMessage", 
            ConsumerRecord.class, 
            Acknowledgment.class
        );
        
        // Crear un ConsumerRecord simulado
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            topic, 1, 39398L, "key", message
        );
        
        // Invocar el método manualmente
        handleMessageMethod.setAccessible(true);
        handleMessageMethod.invoke(kafkaService, record, mockAcknowledgment);
        
        // Then - Verificar que se llamó a acknowledge()
        verify(mockAcknowledgment, times(1)).acknowledge();
    }

    @Test
    void testAcknowledgmentCalledOnProcessingFailure() throws Exception {
        // Given
        String message = createSampleMessage();
        
        // Crear un mock de Acknowledgment
        Acknowledgment mockAcknowledgment = mock(Acknowledgment.class);
        
        // Configurar procesamiento fallido
        when(orchestratorService.processMessage(message)).thenReturn(false);
        
        // When - Invocar manualmente el método handleMessage
        Method handleMessageMethod = ReflectionTestUtils.findMethod(
            KafkaService.class, 
            "handleMessage", 
            ConsumerRecord.class, 
            Acknowledgment.class
        );
        
        // Crear un ConsumerRecord simulado
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            topic, 1, 39398L, "key", message
        );
        
        // Invocar el método manualmente
        handleMessageMethod.setAccessible(true);
        handleMessageMethod.invoke(kafkaService, record, mockAcknowledgment);
        
        // Then - Verificar que se llamó a acknowledge() incluso con procesamiento fallido
        verify(mockAcknowledgment, times(1)).acknowledge();
    }

    @Test
    void testAcknowledgmentCalledOnException() throws Exception {
        // Given
        String message = createSampleMessage();
        
        // Crear un mock de Acknowledgment
        Acknowledgment mockAcknowledgment = mock(Acknowledgment.class);
        
        // Configurar excepción en procesamiento
        when(orchestratorService.processMessage(message)).thenThrow(new RuntimeException("Error de procesamiento"));
        
        // When - Invocar manualmente el método handleMessage
        Method handleMessageMethod = ReflectionTestUtils.findMethod(
            KafkaService.class, 
            "handleMessage", 
            ConsumerRecord.class, 
            Acknowledgment.class
        );
        
        // Crear un ConsumerRecord simulado
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            topic, 1, 39398L, "key", message
        );
        
        // Invocar el método manualmente
        handleMessageMethod.setAccessible(true);
        handleMessageMethod.invoke(kafkaService, record, mockAcknowledgment);
        
        // Then - Verificar que se llamó a acknowledge() incluso con excepción
        verify(mockAcknowledgment, times(1)).acknowledge();
    }
}
