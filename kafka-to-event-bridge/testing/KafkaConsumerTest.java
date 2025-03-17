package com.example.kafka.consumer;

import com.example.kafka.service.MessageService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class KafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    private final MessageService messageService;
    private final KafkaMessageListenerContainer<String, String> container;
    
    @Value("${kafka.topic.name}")
    private String topic;
    
    // Guardar los acknowledgments pendientes para procesamiento posterior
    private final Map<String, Acknowledgment> pendingAcknowledgments = new ConcurrentHashMap<>();
    
    @Autowired
    public KafkaConsumer(MessageService messageService, 
                         KafkaListenerContainerFactory<KafkaMessageListenerContainer<String, String>> containerFactory) {
        this.messageService = messageService;
        
        // Configurar el contenedor del listener
        this.container = containerFactory.createContainer(topic);
        
        // Configurar el listener de mensajes con acknowledgment manual
        this.container.setupMessageListener(new AcknowledgingMessageListenerAdapter(this::handleMessage));
    }
    
    /**
     * Clase adaptadora para el listener de mensajes con confirmación manual
     */
    public class AcknowledgingMessageListenerAdapter implements AcknowledgingMessageListener<String, String> {
        private final AcknowledgingMessageListener<String, String> delegate;
        
        public AcknowledgingMessageListenerAdapter(AcknowledgingMessageListener<String, String> delegate) {
            this.delegate = delegate;
        }
        
        @Override
        public void onMessage(ConsumerRecord<String, String> data, Acknowledgment acknowledgment) {
            delegate.onMessage(data, acknowledgment);
        }
    }
    
    /**
     * Método que maneja los mensajes recibidos del tópico de Kafka
     * @param record El registro consumido de Kafka
     * @param acknowledgment El objeto para confirmar el procesamiento del mensaje
     */
    public void handleMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        String message = record.value();
        String topic = record.topic();
        Integer partition = record.partition();
        Long offset = record.offset();
        
        // Crea un ID único para este mensaje
        String messageId = topic + "-" + partition + "-" + offset;
        
        logger.info("Mensaje recibido: '{}' del tópico '{}', partición '{}', offset '{}'", 
                message, topic, partition, offset);
        
        // Guardar el acknowledgment para confirmación posterior
        pendingAcknowledgments.put(messageId, acknowledgment);
        
        boolean processingResult = messageService.processMessage(message);
        
        if (processingResult) {
            logger.info("Mensaje procesado exitosamente, confirmando recepción");
            acknowledgeMessage(messageId);
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
     * Añade un acknowledgment pendiente (usado para pruebas)
     */
    public void addPendingAcknowledgment(String messageId, Acknowledgment acknowledgment) {
        pendingAcknowledgments.put(messageId, acknowledgment);
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
            logger.info("Mensaje con ID {} confirmado", messageId);
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
