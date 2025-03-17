package com.example.kafka.service;

import com.example.kafka.model.PlainTextMessage;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.UUID;

@Service
public class MessageService {
    private static final Logger logger = LoggerFactory.getLogger(MessageService.class);

    private final AvroJsonService avroJsonService;
    
    @Autowired
    public MessageService(AvroJsonService avroJsonService) {
        this.avroJsonService = avroJsonService;
    }

    /**
     * Procesa un mensaje recibido como texto plano
     * @param message El mensaje a procesar
     * @return true si el procesamiento fue exitoso
     */
    public boolean processMessage(String message) {
        try {
            logger.info("Procesando mensaje: {}", message);
            
            // Realizar cualquier lógica de negocio con el mensaje
            // ...
            
            // Procesar el mensaje con Avro y convertirlo a JSON
            JsonNode jsonResult = avroJsonService.processMessage(message);
            
            // Obtener la representación JSON como string para logging o almacenamiento
            String jsonString = avroJsonService.getJsonString(jsonResult);
            logger.info("Mensaje procesado y convertido. Resultado: {}", jsonString);
            
            return true;
        } catch (Exception e) {
            logger.error("Error al procesar el mensaje: {}", message, e);
            return false;
        }
    }
    
    /**
     * Procesa y transforma el mensaje, creando un objeto PlainTextMessage
     * @param message El mensaje como texto plano
     * @param source El origen del mensaje (opcional)
     * @return true si el procesamiento fue exitoso
     */
    public boolean processMessageWithMetadata(String message, String source) {
        try {
            logger.info("Procesando mensaje con metadata: {}", message);
            
            // Crear un objeto con metadatos
            PlainTextMessage plainTextMessage = PlainTextMessage.builder()
                    .id(UUID.randomUUID().toString())
                    .content(message)
                    .timestamp(System.currentTimeMillis())
                    .source(source != null ? source : "kafka-consumer-service")
                    .properties(new HashMap<>())
                    .build();
            
            // Realizar cualquier lógica de negocio con el mensaje
            // ...
            
            // Procesar el mensaje con Avro y convertirlo a JSON
            JsonNode jsonResult = avroJsonService.processMessage(plainTextMessage);
            
            // Obtener la representación JSON como string para logging o almacenamiento
            String jsonString = avroJsonService.getJsonString(jsonResult);
            logger.info("Mensaje con metadata procesado y convertido. Resultado: {}", jsonString);
            
            return true;
        } catch (Exception e) {
            logger.error("Error al procesar el mensaje con metadata: {}", message, e);
            return false;
        }
    }
}
