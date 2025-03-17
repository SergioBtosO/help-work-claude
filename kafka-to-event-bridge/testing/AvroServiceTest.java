package com.example.kafka.service;

import com.example.kafka.avro.KafkaMessage;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@SpringBootTest
public class AvroServiceTest {

    @Mock
    private SchemaRegistryClient schemaRegistryClient;
    
    @InjectMocks
    private AvroService avroService;
    
    @BeforeEach
    public void setUp() {
        // Configurar valores para las propiedades inyectadas
        ReflectionTestUtils.setField(avroService, "avroSubject", "test-kafka-message-value");
        
        // Usar un mock de SchemaRegistryClient
        SchemaRegistryClient mockClient = new MockSchemaRegistryClient();
        ReflectionTestUtils.setField(avroService, "schemaRegistryClient", mockClient);
        
        // Reinicializar el avroSerializer con el mock
        ReflectionTestUtils.setField(avroService, "avroSerializer", null);
    }
    
    @Test
    public void testConvertToAvro() {
        // Given
        String plainTextMessage = "Test message for AVRO conversion";
        
        // When
        KafkaMessage result = avroService.convertToAvro(plainTextMessage);
        
        // Then
        assertNotNull(result);
        assertEquals(plainTextMessage, result.getContent().toString());
        assertNotNull(result.getId());
        assertNotNull(result.getTimestamp());
        assertEquals("kafka-consumer-service", result.getSource().toString());
        assertNotNull(result.getProperties());
    }
    
    @Test
    public void testProcessAndReplaceMessage() {
        // Given
        String plainTextMessage = "Test message for processing";
        
        // When - Then
        // Este método requiere un entorno más complejo para probarse completamente
        // Aquí solo verificamos que no lance excepciones
        assertDoesNotThrow(() -> avroService.processAndReplaceMessage(plainTextMessage));
    }
}
