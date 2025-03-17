package com.example.kafka.consumer;

import com.example.kafka.service.MessageService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Service
public class KafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    private final MessageService messageService;
    private final KafkaMessageListenerContainer<String, String> container;
    
    @Value("${kafka.topic.name}")
    private String topic;

    @Autowired
    public KafkaConsumer(MessageService messageService, 
                         org.springframework.kafka.config.KafkaListenerContainerFactory<KafkaMessageListenerContainer<String, String>> containerFactory) {
        this.messageService = messageService;
        
        // Configurar el contenedor del listener
        ContainerProperties containerProperties = new ContainerProperties(topic);
        this.container = containerFactory.createContainer(topic);
        
        // Configurar el listener de mensajes
        this.container.setupMessageListener((MessageListener<String, String>) this::handleMessage);
    }
    
    /**
     * Método que maneja los mensajes recibidos del tópico de Kafka
     * @param record El registro consumido de Kafka
     */
    private void handleMessage(ConsumerRecord<String, String> record) {
        String message = record.value();
        String topic = record.topic();
        Integer partition = record.partition();
        Long offset = record.offset();
        
        logger.info("Mensaje recibido: '{}' del tópico '{}', partición '{}', offset '{}'", 
                message, topic, partition, offset);
        
        boolean processingResult = messageService.processMessage(message);
        
        if (processingResult) {
            logger.info("Mensaje procesado exitosamente");
        } else {
            logger.warn("Error en el procesamiento del mensaje");
        }
    }
    
    /**
     * Inicia el contenedor del listener cuando se inicializa el servicio
     */
    @PostConstruct
    public void start() {
        logger.info("Iniciando el consumidor de Kafka para el tópico: {}", topic);
        container.start();
    }
    
    /**
     * Detiene el contenedor del listener cuando se destruye el servicio
     */
    @PreDestroy
    public void stop() {
        logger.info("Deteniendo el consumidor de Kafka");
        container.stop();
    }
}
