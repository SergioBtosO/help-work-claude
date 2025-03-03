package com.empresa.connector.service;

/**
 * Service interface for Avro Schema operations
 */
public interface AvroService {
    
    /**
     * Transforms message attributes with Avro schema information
     * 
     * @param payload The raw message payload
     * @param topic The Kafka topic
     * @param offset The message offset
     * @return The transformed message
     */
    String transformAttributes(String payload, String topic, Long offset);
    
    /**
     * Replaces ID with Avro schema in a message
     * 
     * @param message The original message
     * @return Message with ID replaced by Avro schema
     */
    String replaceIdWithAvroSchema(String message);
    
    /**
     * Retrieves the latest schema for a subject from Schema Registry
     * 
     * @param subject The schema subject
     * @return The schema as a string, or null if not found
     */
    String getLatestSchema(String subject);
    
    /**
     * Serializes data according to a specific Avro schema
     * 
     * @param data The data to serialize
     * @param schemaStr The schema string
     * @return The serialized data
     */
    byte[] serializeWithSchema(Object data, String schemaStr);
    
    /**
     * Deserializes Avro data
     * 
     * @param bytes The serialized data
     * @param schemaStr The schema string
     * @return The deserialized object
     */
    Object deserializeWithSchema(byte[] bytes, String schemaStr);
}

package com.empresa.connector.service.impl;

import com.empresa.connector.service.AvroService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
public class AvroServiceImpl implements AvroService {

    private final SchemaRegistryClient schemaRegistryClient;
    private final ObjectMapper objectMapper;
    
    @Value("${kafka.schema-registry.base-url}")
    private String schemaRegistryUrl;
    
    @Value("${kafka.schema-registry.username}")
    private String username;
    
    @Value("${kafka.schema-registry.password}")
    private String password;

    public AvroServiceImpl(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        
        // Configure Schema Registry Client
        Map<String, Object> configs = new HashMap<>();
        configs.put(SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
        configs.put(SchemaRegistryClientConfig.USER_INFO_CONFIG, username + ":" + password);
        
        this.schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100, configs);
    }

    @Override
    public String transformAttributes(String payload, String topic, Long offset) {
        try {
            // Parse the original payload
            JsonNode rootNode = objectMapper.readTree(payload);
            
            // This would be the place to add or modify attributes based on the topic and offset
            // In a real implementation, you would transform the message according to business rules
            
            // For demonstration, we'll add metadata about the processing
            if (rootNode instanceof ObjectNode) {
                ObjectNode objNode = (ObjectNode) rootNode;
                objNode.put("_kafka_topic", topic);
                objNode.put("_kafka_offset", offset);
                objNode.put("_processing_timestamp", System.currentTimeMillis());
            }
            
            return objectMapper.writeValueAsString(rootNode);
        } catch (Exception e) {
            log.error("Error transforming Avro attributes: {}", e.getMessage(), e);
            return payload; // Return original payload on error
        }
    }

    @Override
    public String replaceIdWithAvroSchema(String message) {
        try {
            // Parse the original message
            JsonNode rootNode = objectMapper.readTree(message);
            
            // Check if ID field exists
            if (rootNode.has("id")) {
                String id = rootNode.get("id").asText();
                
                // Get the schema for this message type
                String subject = "SBNA.00002517.MIP_INS_HIST_EJ.MODIFY.AVRO-value";
                String schemaStr = getLatestSchema(subject);
                
                if (schemaStr != null) {
                    Schema schema = new Schema.Parser().parse(schemaStr);
                    
                    // Create Avro record with the data
                    GenericRecord avroRecord = createAvroRecord(schema, id, rootNode);
                    
                    // Serialize the record
                    byte[] avroData = serializeWithSchema(avroRecord, schemaStr);
                    
                    // Convert to Base64 for string representation
                    String avroBase64 = Base64.getEncoder().encodeToString(avroData);
                    
                    // Replace ID with Avro data in the original message
                    ObjectNode modifiedNode = (ObjectNode) rootNode;
                    modifiedNode.remove("id");
                    modifiedNode.put("avroData", avroBase64);
                    
                    return objectMapper.writeValueAsString(modifiedNode);
                }
            }
            
            // If no ID or schema found, return the original message
            return message;
        } catch (Exception e) {
            log.error("Error replacing ID with Avro schema: {}", e.getMessage(), e);
            return message; // Return original message on error
        }
    }

    @Override
    public String getLatestSchema(String subject) {
        try {
            SchemaMetadata metadata = schemaRegistryClient.getLatestSchemaMetadata(subject);
            return metadata.getSchema();
        } catch (Exception e) {
            log.error("Error getting schema from registry: {}", e.getMessage(), e);
            return null;
        }
    }

    @Override
    public byte[] serializeWithSchema(Object data, String schemaStr) {
        try {
            if (!(data instanceof GenericRecord)) {
                throw new IllegalArgumentException("Data must be a GenericRecord");
            }
            
            GenericRecord record = (GenericRecord) data;
            Schema schema = new Schema.Parser().parse(schemaStr);
            
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            
            // Write Confluent Schema Registry magic byte
            outputStream.write(0);
            
            // Write schema ID (using 0 as a placeholder)
            ByteBuffer buffer = ByteBuffer.allocate(4);
            buffer.putInt(0); // In production, get the actual schema ID from registry
            outputStream.write(buffer.array());
            
            // Write the Avro data
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
            writer.write(record, encoder);
            encoder.flush();
            
            return outputStream.toByteArray();
        } catch (Exception e) {
            log.error("Error serializing with Avro schema: {}", e.getMessage(), e);
            throw new RuntimeException("Error serializing with Avro schema", e);
        }
    }

    @Override
    public Object deserializeWithSchema(byte[] bytes, String schemaStr) {
        try {
            // Skip Confluent Schema Registry magic byte and schema ID
            bytes = ByteBuffer.wrap(bytes).position(5).array();
            
            Schema schema = new Schema.Parser().parse(schemaStr);
            GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);
            
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(
                    new ByteArrayInputStream(bytes), null);
            
            return reader.read(null, decoder);
        } catch (Exception e) {
            log.error("Error deserializing with Avro schema: {}", e.getMessage(), e);
            throw new RuntimeException("Error deserializing with Avro schema", e);
        }
    }
    
    /**
     * Helper method to create an Avro record from JSON data
     */
    private GenericRecord createAvroRecord(Schema schema, String id, JsonNode data) {
        GenericRecord record = new GenericData.Record(schema);
        
        // Populate fields from schema
        for (Schema.Field field : schema.getFields()) {
            String fieldName = field.name();
            
            if (data.has(fieldName)) {
                JsonNode fieldValue = data.get(fieldName);
                record.put(fieldName, convertJsonValueToAvro(fieldValue, field.schema()));
            } else if (fieldName.equals("id")) {
                // Special handling for ID field if not in data
                record.put(fieldName, id);
            }
        }
        
        return record;
    }
    
    /**
     * Helper method to convert JSON values to appropriate Avro types
     */
    private Object convertJsonValueToAvro(JsonNode value, Schema schema) {
        switch (schema.getType()) {
            case STRING:
                return value.asText();
            case INT:
                return value.asInt();
            case LONG:
                return value.asLong();
            case DOUBLE:
                return value.asDouble();
            case BOOLEAN:
                return value.asBoolean();
            case NULL:
                return null;
            default:
                return value.asText();
        }
    }
}

package com.empresa.connector.service.impl;

import com.empresa.connector.mapper.KafkaToEventBridgeMapper;
import com.empresa.connector.model.kafka.KafkaPaymentMessage;
import com.empresa.connector.model.eventbridge.EventBridgeMessage;
import com.empresa.connector.service.AvroService;
import com.empresa.connector.service.TransformationService;
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
    private final AvroService avroService;
    
    @Value("${validation.codesta2}")
    private String validCodesta2;
    
    @Override
    public String transformKafkaAttributes(String payload, String topic, Long offset) {
        try {
            return avroService.transformAttributes(payload, topic, offset);
        } catch (Exception e) {
            log.error("Error transforming Kafka attributes: {}", e.getMessage(), e);
            return payload; // Return original payload on error
        }
    }
    
    @Override
    public String replaceIdWithAvroSchema(String message) {
        try {
            return avroService.replaceIdWithAvroSchema(message);
        } catch (Exception e) {
            log.error("Error replacing ID with Avro schema: {}", e.getMessage(), e);
            return message; // Return original message on error
        }
    }
    
    // Resto de los métodos se mantienen igual...
}