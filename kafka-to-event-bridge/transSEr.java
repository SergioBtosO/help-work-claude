package com.empresa.connector.service;

import com.empresa.connector.model.kafka.KafkaPaymentMessage;
import com.empresa.connector.model.eventbridge.EventBridgeMessage;

/**
 * Service interface for message transformations
 */
public interface TransformationService {
    
    /**
     * Transforms Kafka message attributes
     */
    String transformKafkaAttributes(String payload, String topic, Long offset);
    
    /**
     * Replaces ID with Avro schema in the message
     */
    String replaceIdWithAvroSchema(String message);
    
    /**
     * Transforms a message to JSON format
     */
    String transformToJson(String message);
    
    /**
     * Transforms AWS destiny in the message
     */
    String transformAwsDestiny(String message);
    
    /**
     * Parses a Kafka message from JSON
     */
    KafkaPaymentMessage parseKafkaMessage(String kafkaMessageJson);
    
    /**
     * Transforms a KafkaPaymentMessage to EventBridgeMessage
     */
    EventBridgeMessage transformToEventBridgeMessage(KafkaPaymentMessage kafkaMessage);
    
    /**
     * Converts an EventBridgeMessage to JSON
     */
    String toEventBridgeJson(EventBridgeMessage eventBridgeMessage);
    
    /**
     * Complete transformation from Kafka JSON to EventBridge JSON
     */
    String transformKafkaToEventBridge(String kafkaMessageJson);
}

package com.empresa.connector.service.impl;

import com.empresa.connector.mapper.KafkaToEventBridgeMapper;
import com.empresa.connector.model.kafka.KafkaPaymentMessage;
import com.empresa.connector.model.eventbridge.EventBridgeMessage;
import com.empresa.connector.service.TransformationService;
import com.empresa.connector.transformer.AvroSchemaTransformer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransformationServiceImpl implements TransformationService {
    
    private final KafkaToEventBridgeMapper mapper;
    private final ObjectMapper objectMapper;
    private final AvroSchemaTransformer avroSchemaTransformer;
    
    @Value("${validation.codesta2}")
    private String validCodesta2;
    
    @Override
    public String transformKafkaAttributes(String payload, String topic, Long offset) {
        try {
            return avroSchemaTransformer.transformAttributes(payload, topic, offset);
        } catch (Exception e) {
            log.error("Error transforming Kafka attributes: {}", e.getMessage(), e);
            return payload; // Return original payload on error
        }
    }
    
    @Override
    public String replaceIdWithAvroSchema(String message) {
        try {
            return avroSchemaTransformer.replaceIdWithAvroSchema(message);
        } catch (Exception e) {
            log.error("Error replacing ID with Avro schema: {}", e.getMessage(), e);
            return message; // Return original message on error
        }
    }
    
    @Override
    public String transformToJson(String message) {
        try {
            // Check if already valid JSON
            objectMapper.readTree(message);
            return message;
        } catch (Exception e) {
            log.warn("Message is not valid JSON, attempting to convert: {}", e.getMessage());
            try {
                // Try to convert to JSON
                Object obj = message;
                return objectMapper.writeValueAsString(obj);
            } catch (Exception ex) {
                log.error("Error transforming to JSON: {}", ex.getMessage(), ex);
                return message; // Return original message on error
            }
        }
    }
    
    @Override
    public String transformAwsDestiny(String message) {
        try {
            // Parse message to JSON
            JsonNode rootNode = objectMapper.readTree(message);
            
            // Check if awsDestiny is already set
            if (rootNode.has("awsDestiny")) {
                return message;
            }
            
            // Set default awsDestiny value
            ((ObjectNode) rootNode).put("awsDestiny", "aws1");
            
            // Convert back to string
            return objectMapper.writeValueAsString(rootNode);
        } catch (Exception e) {
            log.error("Error transforming AWS destiny: {}", e.getMessage(), e);
            return message; // Return original message on error
        }
    }
    
    @Override
    public KafkaPaymentMessage parseKafkaMessage(String kafkaMessageJson) {
        try {
            return objectMapper.readValue(kafkaMessageJson, KafkaPaymentMessage.class);
        } catch (Exception e) {
            log.error("Error parsing Kafka message: {}", e.getMessage(), e);
            throw new RuntimeException("Error parsing Kafka message", e);
        }
    }
    
    @Override
    public EventBridgeMessage transformToEventBridgeMessage(KafkaPaymentMessage kafkaMessage) {
        return mapper.toEventBridgeMessage(kafkaMessage);
    }
    
    @Override
    public String toEventBridgeJson(EventBridgeMessage eventBridgeMessage) {
        try {
            return objectMapper.writeValueAsString(eventBridgeMessage);
        } catch (Exception e) {
            log.error("Error converting EventBridgeMessage to JSON: {}", e.getMessage(), e);
            throw new RuntimeException("Error converting EventBridgeMessage to JSON", e);
        }
    }
    
    @Override
    public String transformKafkaToEventBridge(String kafkaMessageJson) {
        try {
            KafkaPaymentMessage kafkaMessage = parseKafkaMessage(kafkaMessageJson);
            EventBridgeMessage eventBridgeMessage = transformToEventBridgeMessage(kafkaMessage);
            return toEventBridgeJson(eventBridgeMessage);
        } catch (Exception e) {
            log.error("Error transforming Kafka to EventBridge: {}", e.getMessage(), e);
            throw new RuntimeException("Error transforming Kafka to EventBridge", e);
        }
    }
}