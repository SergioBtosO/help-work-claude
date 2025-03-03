package com.empresa.connector.service;

import com.empresa.connector.model.kafka.KafkaPaymentMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProcessingService {

    private final TransformationService transformationService;
    private final EventBridgeService eventBridgeService;
    private final RedisService redisService;
    
    @Value("${validation.codesta2}")
    private String validCodesta2;

    @KafkaListener(
        topics = "${kafka.consumer.topic-pattern}",
        groupId = "${kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void receivePaymentEvent(
            @Payload String payload,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition,
            @Header(KafkaHeaders.OFFSET) Long offset,
            Acknowledgment ack) {
        
        Instant startTime = Instant.now();
        
        try {
            // Log INPUT attributes
            log.info("Message received - Topic: {}, Partition: {}, Offset: {}", 
                    topic, partition, offset);
            
            // Log INPUT payload
            log.info("Payload received: {}", payload);
            
            // Process the event
            processKafkaEvent(payload, topic, partition, offset);
            
            // Acknowledge successful processing
            ack.acknowledge();
            
            // Log OUTPUT execution time
            long executionTime = Instant.now().toEpochMilli() - startTime.toEpochMilli();
            log.info("Total execution time: {} ms", executionTime);
            
        } catch (Exception e) {
            log.error("Error processing Kafka message: {}", e.getMessage(), e);
            // No acknowledgment to retry according to retry policy
        }
    }
    
    /**
     * Process a Kafka event from start to finish
     */
    private void processKafkaEvent(String payload, String topic, Integer partition, Long offset) {
        try {
            // Save original payload
            String originalPayload = payload;
            
            // Transform Kafka attributes
            String transformedMessage = transformationService.transformKafkaAttributes(payload, topic, offset);
            log.debug("Transformed message: {}", transformedMessage);
            
            // Parse Kafka message
            KafkaPaymentMessage kafkaMessage = transformationService.parseKafkaMessage(transformedMessage);
            
            // Replace ID with AVRO schema
            String processedMessage = transformationService.replaceIdWithAvroSchema(transformedMessage);
            
            // Transform to JSON
            String jsonPayload = transformationService.transformToJson(processedMessage);
            log.debug("JSON payload: {}", jsonPayload);
            
            // Validate CODESTA2
            if (isValidCodesta2(kafkaMessage)) {
                log.info("Valid CODESTA2: '{}'", validCodesta2);
                
                // Transform for EventBridge
                String eventBridgeMessage = transformationService.transformKafkaToEventBridge(jsonPayload);
                
                // Apply AWS destiny transformation
                String transformedEventBridgeMessage = transformationService.transformAwsDestiny(eventBridgeMessage);
                
                // Determine destinations
                boolean sendToAws1 = shouldSendToAws1(transformedEventBridgeMessage);
                boolean sendToAws2 = shouldSendToAws2(transformedEventBridgeMessage);
                
                // Send to AWS1 if needed
                if (sendToAws1) {
                    log.info("Sending to AWS1");
                    String result = eventBridgeService.sendToEventBridgeWithAws1Config(transformedEventBridgeMessage);
                    log.info("AWS1 sending result: {}", result);
                } else {
                    log.info("Not sending to AWS1");
                }
                
                // Send to AWS2 if needed
                if (sendToAws2) {
                    log.info("Sending to AWS2");
                    String result = eventBridgeService.sendToEventBridgeWithAws2Config(transformedEventBridgeMessage);
                    log.info("AWS2 sending result: {}", result);
                } else {
                    log.info("Not sending to AWS2");
                }
                
            } else {
                log.warn("Invalid CODESTA2: {}. Message will not be processed.", kafkaMessage.getCodesta2());
            }
            
            // Send ACK message
            eventBridgeService.sendAckMessage();
            
        } catch (Exception e) {
            log.error("Error processing Kafka event: {}", e.getMessage(), e);
            throw new RuntimeException("Error processing Kafka message", e);
        }
    }
    
    /**
     * Check if CODESTA2 is valid
     */
    private boolean isValidCodesta2(KafkaPaymentMessage message) {
        return message != null && validCodesta2.equals(message.getCodesta2());
    }
    
    /**
     * Determine if message should be sent to AWS1
     */
    private boolean shouldSendToAws1(String message) {
        // Default to true unless explicitly specified for AWS2 only
        return !message.contains("\"awsDestiny\":\"aws2\"") || message.contains("\"awsDestiny\":\"aws1\"");
    }
    
    /**
     * Determine if message should be sent to AWS2
     */
    private boolean shouldSendToAws2(String message) {
        return message.contains("\"awsDestiny\":\"aws2\"");
    }
}