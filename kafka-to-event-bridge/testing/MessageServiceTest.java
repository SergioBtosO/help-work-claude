package com.example.kafka.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
public class MessageServiceTest {

    @Autowired
    private MessageService messageService;

    @Test
    void testProcessMessageSuccess() {
        // Given
        String message = "Mensaje de prueba exitoso";

        // When
        boolean result = messageService.processMessage(message);

        // Then
        assertThat(result).isTrue();
    }

    @Test
    void testProcessMessageWithNullMessage() {
        // Given
        String message = null;

        // When
        boolean result = messageService.processMessage(message);

        // Then
        assertThat(result).isTrue(); // La implementación actual maneja mensajes nulos
    }
}
