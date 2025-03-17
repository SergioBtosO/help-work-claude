package com.example.kafka.consumer;

import com.example.kafka.service.MessageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    private final MessageService messageService;

    @Autowired
    public KafkaConsumer(MessageService messageService) {
        this.messageService = messageService;
    }

    /**
     * Método que escucha mensajes del tópico de Kafka configurado
     * @param message El mensaje recibido del tópico
     * @param topic El nombre del tópico
     * @param partition La partición de la que se recibió el mensaje
     * @param offset El offset del mensaje
     */
    @KafkaListener(topics = "${kafka.topic.name}")
    public void listen(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition,
            @Header(KafkaHeaders.OFFSET) Long offset) {
        
        logger.info("Mensaje recibido: '{}' del tópico '{}', partición '{}', offset '{}'", 
                message, topic, partition, offset);
        
        boolean processingResult = messageService.processMessage(message);
        
        if (processingResult) {
            logger.info("Mensaje procesado exitosamente");
        } else {
            logger.warn("Error en el procesamiento del mensaje");
        }
    }
}
