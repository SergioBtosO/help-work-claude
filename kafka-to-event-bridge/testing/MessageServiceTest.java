package com.example.kafka.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@SpringBootTest
public class MessageServiceTest {

    @Autowired
    private MessageService messageService;
    
    @MockBean
    private AvroService avroService;

    @Test
    void testProcessMessageSuccess() {
        // Given
        String message = "Mensaje de prueba exitoso";
        when(avroService.processAndReplaceMessage(anyString())).thenReturn(true);

        // When
        boolean result = messageService.processMessage(message);

        // Then
        assertThat(result).isTrue();
    }

    @Test
    void testProcessMessageWithAvroFailure() {
        // Given
        String message = "Mensaje con fallo en AVRO";
        when(avroService.processAndReplaceMessage(anyString())).thenReturn(false);

        // When
        boolean result = messageService.processMessage(message);

        // Then
        assertThat(result).isTrue(); // El procesamiento principal sigue siendo exitoso
    }
}
