/**
 * ESTRUCTURA DEL PROYECTO SPRING BOOT PARA MIGRACIÓN DE MULESOFT
 */

/**
 * 1. ESTRUCTURA DE DIRECTORIOS
 */

com.empresa.connector
├── KafkaToEventbridgePaymentsConnectorApplication.java  // Clase principal de Spring Boot
├── config/                      // Configuraciones
│   ├── KafkaConfig.java         // Configuración de Kafka consumer/producer
│   ├── AwsConfig.java           // Configuración de AWS/EventBridge
│   ├── RedisConfig.java         // Configuración de Redis
│   └── AppConfig.java           // Otras configuraciones generales
├── listener/                    // Equivalente a "listeners" en Mulesoft
│   └── KafkaEventListener.java  // Consumidor de mensajes Kafka
├── handler/                     // Equivalente a "handlers" en Mulesoft
│   └── KafkaToEventBridgeHandler.java  // Procesa los eventos
├── orchestrator/                // Equivalente a "orchestrators" en Mulesoft
│   ├── EventBridgeOrchestrator.java
│   └── KafkaMessageToEventBridgeOrchestrator.java
├── transformer/                 // Para transformaciones de mensajes
│   ├── AvroSchemaTransformer.java
│   ├── JsonTransformer.java
│   └── MessageTransformer.java
├── service/                     // Servicios de negocio
│   ├── AwsIamService.java
│   ├── EventBridgeService.java
│   └── RedisService.java
├── model/                       // Modelos de datos
│   ├── PaymentEvent.java
│   ├── EventBridgeMessage.java
│   └── AwsCredentials.java
├── exception/                   // Manejo de excepciones
│   ├── GlobalExceptionHandler.java
│   └── ConnectorException.java
└── util/                        // Utilidades
    ├── LoggerUtil.java
    └── AwsUtil.java


/**
 * 2. IMPLEMENTACIONES CLAVE
 */

/**
 * 2.1 KAFKA LISTENER (Basado en Image 1)
 */
package com.empresa.connector.listener;

import com.empresa.connector.handler.KafkaToEventBridgeHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import java.time.Instant;

@Slf4j
@Component
public class KafkaEventListener {

    private final KafkaToEventBridgeHandler handler;

    public KafkaEventListener(KafkaToEventBridgeHandler handler) {
        this.handler = handler;
    }

    @KafkaListener(topics = "${kafka.topics.payments}", 
                  groupId = "${kafka.consumer.group-id}",
                  containerFactory = "kafkaListenerContainerFactory")
    public void receivePaymentEvent(
            @Payload String payload,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) Integer partition,
            @Header(KafkaHeaders.OFFSET) Long offset) {
        
        Instant startTime = Instant.now();
        
        // Log INPUT attributes (como se ve en la Image 1)
        log.info("Mensaje recibido - Topic: {}, Partition: {}, Offset: {}", 
                topic, partition, offset);
        
        // Log INPUT payload (como se ve en la Image 1)
        log.info("Payload recibido: {}", payload);
        
        // Flow Reference: Call handler
        handler.processEvent(payload, topic, partition, offset);
        
        // Log OUTPUT execution time (como se ve en la Image 1)
        long executionTime = Instant.now().toEpochMilli() - startTime.toEpochMilli();
        log.info("Tiempo de ejecución total: {} ms", executionTime);
    }
}

/**
 * 2.2 HANDLER (Basado en Image 2)
 */
package com.empresa.connector.handler;

import com.empresa.connector.orchestrator.KafkaMessageToEventBridgeOrchestrator;
import com.empresa.connector.transformer.AvroSchemaTransformer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaToEventBridgeHandler {

    private final KafkaMessageToEventBridgeOrchestrator orchestrator;
    private final AvroSchemaTransformer avroTransformer;

    public KafkaToEventBridgeHandler(
            KafkaMessageToEventBridgeOrchestrator orchestrator,
            AvroSchemaTransformer avroTransformer) {
        this.orchestrator = orchestrator;
        this.avroTransformer = avroTransformer;
    }

    public void processEvent(String payload, String topic, Integer partition, Long offset) {
        try {
            // Set Variable: Save originalPayload (como se ve en la Image 2)
            String originalPayload = payload;
            
            // Transform Message: attributes, key, init, topic & ackId (como se ve en la Image 2)
            String transformedMessage = avroTransformer.transformAttributes(payload, topic, offset);
            
            // Flow Reference: Send Kafka message to EventBridge
            orchestrator.processAndSendToEventBridge(transformedMessage, originalPayload);
            
        } catch (Exception e) {
            log.error("Error en el procesamiento del evento: {}", e.getMessage(), e);
            throw new RuntimeException("Error procesando mensaje Kafka", e);
        }
    }
}

/**
 * 2.3 KAFKA MESSAGE TO EVENTBRIDGE ORCHESTRATOR (Basado en Image 3)
 */
package com.empresa.connector.orchestrator;

import com.empresa.connector.service.EventBridgeService;
import com.empresa.connector.transformer.JsonTransformer;
import com.empresa.connector.transformer.MessageTransformer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaMessageToEventBridgeOrchestrator {

    private final EventBridgeService eventBridgeService;
    private final JsonTransformer jsonTransformer;
    private final MessageTransformer messageTransformer;

    public KafkaMessageToEventBridgeOrchestrator(
            EventBridgeService eventBridgeService,
            JsonTransformer jsonTransformer,
            MessageTransformer messageTransformer) {
        this.eventBridgeService = eventBridgeService;
        this.jsonTransformer = jsonTransformer;
        this.messageTransformer = messageTransformer;
    }

    public void processAndSendToEventBridge(String message, String originalPayload) {
        // Replace id with AVRO schema
        String processedMessage = messageTransformer.replaceIdWithAvroSchema(message);
        
        // Transform Message payload to JSON
        String jsonPayload = jsonTransformer.transformToJson(processedMessage);
        
        // Log DEBUG payload
        log.debug("Mensaje transformado a JSON: {}", jsonPayload);
        
        // Verificación de CODESTA2
        if (isValidCodesta2(jsonPayload)) {
            log.info("CODESTA2 válido: '13'");
            
            // Transform Message - Set aws and operationId
            String messageWithAwsInfo = messageTransformer.setAwsAndOperationId(jsonPayload);
            
            // Flow Reference - Send message to EventBridge
            eventBridgeService.sendToEventBridge(messageWithAwsInfo);
            
            // Flow Reference - Send ACK
            eventBridgeService.sendAckMessage();
        } else {
            log.warn("CODESTA2 no válido");
            
            // Flow Reference - Send ACK (sólo ACK, sin envío a EventBridge)
            eventBridgeService.sendAckMessage();
        }
    }
    
    private boolean isValidCodesta2(String payload) {
        // Implementación para verificar si G6181_CODESTA2 == "13"
        return payload != null && payload.contains("\"G6181_CODESTA2\":\"13\"");
    }
}

/**
 * 2.4 EVENTBRIDGE ORCHESTRATOR (Basado en Image 4)
 */
package com.empresa.connector.orchestrator;

import com.empresa.connector.service.EventBridgeService;
import com.empresa.connector.transformer.MessageTransformer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class EventBridgeOrchestrator {

    private final EventBridgeService eventBridgeService;
    private final MessageTransformer messageTransformer;

    public EventBridgeOrchestrator(
            EventBridgeService eventBridgeService,
            MessageTransformer messageTransformer) {
        this.eventBridgeService = eventBridgeService;
        this.messageTransformer = messageTransformer;
    }

    public void sendToEventBridge(String message) {
        // Transform Message awsDestiny
        String transformedMessage = messageTransformer.transformAwsDestiny(message);
        
        // Decisión: Send to AWS1?
        boolean sendToAws1 = shouldSendToAws1(transformedMessage);
        
        if (sendToAws1) {
            // Flow Reference - Send to EventBridge with aws1 configuration
            eventBridgeService.sendToEventBridgeWithAws1Config(transformedMessage);
        } else {
            log.info("No se envía a AWS1");
        }
        
        // Decisión: Send to AWS2?
        boolean sendToAws2 = shouldSendToAws2(transformedMessage);
        
        if (sendToAws2) {
            // Flow Reference - Send to EventBridge with aws2 configuration
            eventBridgeService.sendToEventBridgeWithAws2Config(transformedMessage);
        } else {
            log.info("No se envía a AWS2");
        }
    }
    
    private boolean shouldSendToAws1(String message) {
        // Implementar lógica para determinar si se debe enviar a AWS1
        return true; // Por defecto, enviar a AWS1
    }
    
    private boolean shouldSendToAws2(String message) {
        // Implementar lógica para determinar si se debe enviar a AWS2
        return false; // Por defecto, no enviar a AWS2
    }
}

/**
 * 2.5 AWS EVENTBRIDGE SERVICE (Basado en Image 5 y 6)
 */
package com.empresa.connector.service;

import com.empresa.connector.model.EventBridgeMessage;
import com.empresa.connector.service.AwsIamService;
import com.empresa.connector.service.RedisService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse;

@Slf4j
@Service
public class EventBridgeService {

    private final EventBridgeClient eventBridgeClient;
    private final AwsIamService awsIamService;
    private final RedisService redisService;
    
    @Value("${aws.eventbridge.source}")
    private String eventSource;
    
    @Value("${aws.eventbridge.detailType}")
    private String detailType;
    
    @Value("${aws.eventbridge.bus1}")
    private String eventBusBus1;
    
    @Value("${aws.eventbridge.bus2}")
    private String eventBusBus2;

    public EventBridgeService(
            EventBridgeClient eventBridgeClient,
            AwsIamService awsIamService,
            RedisService redisService) {
        this.eventBridgeClient = eventBridgeClient;
        this.awsIamService = awsIamService;
        this.redisService = redisService;
    }

    public void sendToEventBridge(String message) {
        try {
            log.info("Enviando mensaje a EventBridge: {}", message);
            
            // Set eventBridgeMessage
            String eventBridgeMessage = prepareMessage(message);
            
            // Set awsTarget
            String awsTarget = extractAwsTarget(message);
            
            // Set redisPartitionIdentifier
            String redisKey = generateRedisKey(message);
            
            // Flow Reference - Send to EventBridge
            sendToEventBridgeWithTarget(eventBridgeMessage, awsTarget, redisKey);
            
        } catch (Exception e) {
            log.error("Error al enviar mensaje a EventBridge: {}", e.getMessage(), e);
            throw new RuntimeException("Error enviando a EventBridge", e);
        }
    }

    public void sendToEventBridgeWithAws1Config(String message) {
        try {
            log.info("Enviando a AWS1");
            
            // Verificar y obtener credenciales
            awsIamService.getAwsCredentials();
            
            PutEventsRequest request = PutEventsRequest.builder()
                .entries(PutEventsRequestEntry.builder()
                    .eventBusName(eventBusBus1)
                    .source(eventSource)
                    .detailType(detailType)
                    .detail(message)
                    .build())
                .build();
                
            PutEventsResponse response = eventBridgeClient.putEvents(request);
            log.info("Respuesta de EventBridge AWS1: {}", response.toString());
            
        } catch (Exception e) {
            // Manejar errores específicos como en Image 5
            log.error("Error al enviar a AWS1: {}", e.getMessage(), e);
            
            // Verificar si es solo para AWS1
            if (isSentJustToAws1(message)) {
                log.error("El mensaje era exclusivo para AWS1, generando error");
                throw new RuntimeException("Error enviando a AWS1", e);
            } else {
                log.warn("El mensaje no era exclusivo para AWS1, continuando flujo");
                // Establecer aws1Status a false
                setAws1StatusToFalse();
            }
        }
    }
    
    public void sendToEventBridgeWithAws2Config(String message) {
        // Implementación similar a AWS1 para AWS2
    }
    
    public void sendToEventBridgeWithTarget(String message, String awsTarget, String redisKey) {
        // Implementar envío basado en target
    }
    
    public void sendAckMessage() {
        // Implementar envío de mensajes ACK
        log.info("Enviando mensaje ACK");
    }
    
    // Métodos auxiliares
    private String prepareMessage(String message) {
        return message; // Aplicar transformaciones si es necesario
    }
    
    private String extractAwsTarget(String message) {
        // Extraer target AWS de mensaje
        return "aws1"; // Por defecto
    }
    
    private String generateRedisKey(String message) {
        // Generar clave para Redis
        return "payment:" + System.currentTimeMillis();
    }
    
    private boolean isSentJustToAws1(String message) {
        // Implementar lógica para verificar si era solo para AWS1
        return true;
    }
    
    private void setAws1StatusToFalse() {
        // Implementar establecimiento de variable aws1Status
    }
}

/**
 * 2.6 REDIS SERVICE (Basado en Image 8)
 */
package com.empresa.connector.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class RedisService {

    private final RedisTemplate<String, String> redisTemplate;
    
    public RedisService(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }
    
    public String getCache(String key) {
        try {
            log.info("START GET Redis: {}", key);
            String value = redisTemplate.opsForValue().get(key);
            
            // Transform Message - Set awsUser
            if (value != null) {
                log.info("Datos encontrados en Redis para key: {}", key);
            } else {
                log.info("No hay datos en Redis para key: {}", key);
            }
            
            log.info("END GET Redis");
            return value;
        } catch (Exception e) {
            log.error("Error en GET Redis: {}", e.getMessage(), e);
            log.info("END GET Redis (con error)");
            return null;
        }
    }
    
    public void setCache(String key, String value, long timeout, TimeUnit unit) {
        try {
            log.info("START SET Redis: {}", key);
            redisTemplate.opsForValue().set(key, value, timeout, unit);
            log.info("END SET Redis");
        } catch (Exception e) {
            log.error("Error en SET Redis: {}", e.getMessage(), e);
            log.info("END SET Redis (con error)");
        }
    }
    
    public void deleteCache(String key) {
        try {
            log.info("START DEL Redis: {}", key);
            redisTemplate.delete(key);
            log.info("END DEL Redis");
        } catch (Exception e) {
            log.error("Error en DEL Redis: {}", e.getMessage(), e);
            log.info("END DEL Redis (con error)");
        }
    }
}

/**
 * 2.7 AWS IAM SERVICE (Basado en Image 6 y 7)
 */
package com.empresa.connector.service;

import com.empresa.connector.model.AwsCredentials;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AwsIamService {

    private final RedisService redisService;
    
    public AwsIamService(RedisService redisService) {
        this.redisService = redisService;
    }
    
    public AwsCredentials getAwsCredentials() {
        try {
            boolean sessionExpired = checkIfSessionExpired();
            
            if (sessionExpired) {
                // Llamar a retrieveSession
                retrieveNewSession();
            }
            
            // Implementar la carga de certificados
            loadCertificates();
            
            // Normalizar iamBody como string
            String normalizedIamBody = normalizeIamBody();
            
            // Set aws host
            String awsHost = setAwsHost();
            
            // Set headers with certificates
            setHeadersWithCertificates();
            
            // Crear y retornar credenciales
            return buildAwsCredentials();
            
        } catch (Exception e) {
            log.error("Error obteniendo credenciales AWS: {}", e.getMessage(), e);
            throw new RuntimeException("Error en IAM Credentials", e);
        }
    }
    
    private boolean checkIfSessionExpired() {
        // Verificar si la sesión ha expirado
        return false; // Por defecto
    }
    
    private void retrieveNewSession() {
        // Implementar retrieval de sesión
        // Usar redis-client:get-cache-flow
        redisService.getCache("aws:session:token");
    }
    
    private void loadCertificates() {
        // Implementar carga de certificados
        // Leer certificado privado y público
    }
    
    private String normalizeIamBody() {
        // Normalizar cuerpo IAM
        return "{}";
    }
    
    private String setAwsHost() {
        // Establecer host AWS
        return "api.aws.com";
    }
    
    private void setHeadersWithCertificates() {
        // Establecer headers con certificados
    }
    
    private AwsCredentials buildAwsCredentials() {
        // Construir y retornar objeto de credenciales
        return new AwsCredentials("accessKey", "secretKey", "sessionToken");
    }
}

/**
 * 2.8 TRANSFORMERS
 */
package com.empresa.connector.transformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AvroSchemaTransformer {

    private final ObjectMapper objectMapper;
    
    public AvroSchemaTransformer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }
    
    public String transformAttributes(String payload, String topic, Long offset) {
        try {
            // Implementar transformación de atributos según Image 2
            return payload;
        } catch (Exception e) {
            log.error("Error transformando atributos AVRO: {}", e.getMessage(), e);
            throw new RuntimeException("Error en transformación AVRO", e);
        }
    }
    
    // Otros métodos específicos para transformaciones AVRO
}

@Slf4j
@Component
public class JsonTransformer {

    private final ObjectMapper objectMapper;
    
    public JsonTransformer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }
    
    public String transformToJson(String payload) {
        try {
            // Transformar a JSON según la lógica del Image 3
            return payload;
        } catch (Exception e) {
            log.error("Error transformando a JSON: {}", e.getMessage(), e);
            throw new RuntimeException("Error en transformación JSON", e);
        }
    }
}

@Slf4j
@Component
public class MessageTransformer {

    private final ObjectMapper objectMapper;
    
    public MessageTransformer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }
    
    public String replaceIdWithAvroSchema(String message) {
        // Implementar reemplazo de ID con schema AVRO (Image 3)
        return message;
    }
    
    public String setAwsAndOperationId(String jsonPayload) {
        // Implementar transformación para AWS y operationId (Image 3)
        return jsonPayload;
    }
    
    public String transformAwsDestiny(String message) {
        // Implementar transformación para awsDestiny (Image 4)
        return message;
    }
}

/**
 * 2.9 CONFIGURACIÓN GLOBAL DE ERROR
 */
package com.empresa.connector.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@Slf4j
@ControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(Exception.class)
    public ResponseEntity<String> handleException(Exception ex) {
        log.error("Error global: {}", ex.getMessage(), ex);
        return new ResponseEntity<>("Error interno del servidor", HttpStatus.INTERNAL_SERVER_ERROR);
    }
    
    @ExceptionHandler(ListenerExecutionFailedException.class)
    public void handleKafkaException(ListenerExecutionFailedException ex) {
        log.error("Error en listener Kafka: {}", ex.getMessage(), ex);
        // Manejar errores de Kafka
    }
}

/**
 * 2.10 CONFIGURACIONES
 */
package com.empresa.connector.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    @Value("${kafka.consumer.group-id}")
    private String groupId;
    
    @Value("${kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;
    
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        return new DefaultKafkaConsumerFactory<>(props);
    }
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}

@Configuration
public class AwsConfig {

    @Value("${aws.region}")
    private String region;
    
    @Bean
    public EventBridgeClient eventBridgeClient() {
        return EventBridgeClient.builder()
                .region(Region.of(region))
                .build();
    }
}

@Configuration
public class RedisConfig {

    @Value("${redis.host}")
    private String redisHost;
    
    @Value("${redis.port}")
    private int redisPort;
    
    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration(redisHost, redisPort);
        return new LettuceConnectionFactory(config);
    }
    
    @Bean
    public RedisTemplate<String, String> redisTemplate() {
        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(redisConnectionFactory());
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new StringRedisSerializer());
        return template;
    }
}

/**
 * 2.11 APPLICATION.PROPERTIES
 */
# src/main/resources/application.properties
server.port=8080

# Kafka Configuration
kafka.bootstrap-servers=localhost:9092
kafka.consumer.group-id=payments-group
kafka.consumer.auto-offset-reset=earliest
kafka.topics.payments=SBNA.00002517.MIP_INS_HIST_EJ.MODIF.AVRO

# AWS Configuration
aws.region=us-east-1
aws.eventbridge.source=payment-service
aws.eventbridge.detailType=PaymentEvent
aws.eventbridge.bus1=payments-event-bus-1
aws.eventbridge.bus2=payments-event-bus-2

# Redis Configuration
redis.host=localhost
redis.port=6379

# Logging Configuration
logging.level.root=INFO
logging.level.com.empresa.connector=DEBUG
logging.pattern.console=%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger{36} - %msg%n