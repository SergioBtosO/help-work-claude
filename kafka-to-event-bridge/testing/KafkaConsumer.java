package com.example.kafka.consumer;

import com.example.kafka.service.MessageService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
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
        this.container = containerFactory.createContainer(topic);
        
        // Configurar el listener de mensajes con acknowledgment manual
        this.container.setupMessageListener((AcknowledgingMessageListener<String, String>) this::handleMessage);
    }
    
    // Guardar los acknowledgments pendientes para procesamiento posterior
    private final Map<String, Acknowledgment> pendingAcknowledgments = new ConcurrentHashMap<>();
    
    /**
     * Método que maneja los mensajes recibidos del tópico de Kafka
     * @param record El registro consumido de Kafka
     * @param acknowledgment El objeto para confirmar el procesamiento del mensaje
     */
    private void handleMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        String message = record.value();
        String topic = record.topic();
        Integer partition = record.partition();
        Long offset = record.offset();
        
        // Crea un ID único para este mensaje
        String messageId = topic + "-" + partition + "-" + offset;
        
        logger.info("Mensaje recibido: '{}' del tópico '{}', partición '{}', offset '{}'", 
                message, topic, partition, offset);
        
        // Guarda el acknowledgment para confirmación posterior
        pendingAcknowledgments.put(messageId, acknowledgment);
        
        boolean processingResult = messageService.processMessage(message);
        
        if (processingResult) {
            logger.info("Mensaje procesado exitosamente, esperando confirmación manual (modo MANUAL)");
            
            // Si queremos confirmación automática después del procesamiento exitoso, descomenta:
            // acknowledgeMessage(messageId);
        } else {
            logger.warn("Error en el procesamiento del mensaje, esperando confirmación manual");
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
    
    /**
     * Confirma manualmente un mensaje por su ID único
     * @param messageId ID único del mensaje (tópico-partición-offset)
     * @return true si el mensaje fue confirmado, false si el ID no existe
     */
    public boolean acknowledgeMessage(String messageId) {
        Acknowledgment acknowledgment = pendingAcknowledgments.get(messageId);
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
            pendingAcknowledgments.remove(messageId);
            logger.info("Mensaje con ID {} confirmado manualmente (modo MANUAL - se enviará en el próximo lote)", messageId);
            return true;
        } else {
            logger.warn("No se encontró acknowledgment para el mensaje con ID {}", messageId);
            return false;
        }
    }
    
    /**
     * Obtiene todos los IDs de mensajes pendientes de confirmación
     * @return Lista de IDs de mensajes pendientes
     */
    public Set<String> getPendingMessageIds() {
        return new HashSet<>(pendingAcknowledgments.keySet());
    }
}
