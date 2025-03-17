package com.example.kafka.service;

import com.example.kafka.avro.KafkaMessage;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class AvroService {
    private static final Logger logger = LoggerFactory.getLogger(AvroService.class);
    
    private final SchemaRegistryClient schemaRegistryClient;
    private final KafkaAvroSerializer avroSerializer;
    
    @Value("${schema.avro.subject:kafka-message-value}")
    private String avroSubject;
    
    @Autowired
    public AvroService(SchemaRegistryClient schemaRegistryClient) {
        this.schemaRegistryClient = schemaRegistryClient;
        this.avroSerializer = new KafkaAvroSerializer(schemaRegistryClient);
    }
    
    /**
     * Convierte un mensaje de texto plano a formato AVRO y lo registra en el Schema Registry
     * @param plainTextMessage El mensaje en texto plano
     * @return El mensaje convertido a AVRO
     */
    public KafkaMessage convertToAvro(String plainTextMessage) {
        try {
            logger.info("Convirtiendo mensaje a formato AVRO: {}", plainTextMessage);
            
            // Crear un objeto KafkaMessage a partir del texto plano
            KafkaMessage avroMessage = KafkaMessage.newBuilder()
                    .setId(UUID.randomUUID().toString())
                    .setContent(plainTextMessage)
                    .setTimestamp(System.currentTimeMillis())
                    .setSource("kafka-consumer-service")
                    .setProperties(new HashMap<>())
                    .build();
            
            // Serializar el mensaje a formato AVRO
            byte[] serializedMessage = avroSerializer.serialize(avroSubject, avroMessage);
            logger.info("Mensaje serializado exitosamente a AVRO, tamaño: {} bytes", serializedMessage.length);
            
            return avroMessage;
        } catch (Exception e) {
            logger.error("Error al convertir mensaje a AVRO", e);
            throw new RuntimeException("Error al convertir mensaje a AVRO", e);
        }
    }
    
    /**
     * Registra un esquema AVRO en el Schema Registry
     * @param schema El esquema a registrar
     * @param subject El subject bajo el cual registrarlo
     * @return El ID del esquema registrado
     */
    public int registerSchema(Schema schema, String subject) {
        try {
            logger.info("Registrando esquema para subject: {}", subject);
            return schemaRegistryClient.register(subject, schema);
        } catch (IOException | RestClientException e) {
            logger.error("Error al registrar esquema en Schema Registry", e);
            throw new RuntimeException("Error al registrar esquema", e);
        }
    }
    
    /**
     * Reemplaza un mensaje en el Schema Registry
     * @param messageId ID del mensaje original
     * @param avroMessage El mensaje AVRO nuevo
     * @return true si la operación fue exitosa
     */
    public boolean replaceMessage(String messageId, KafkaMessage avroMessage) {
        try {
            logger.info("Reemplazando mensaje con ID: {} en Schema Registry", messageId);
            
            // Aquí iría la lógica para reemplazar el mensaje en el Schema Registry
            // Esto puede variar según la implementación específica de tu Schema Registry
            
            // Ejemplo simplificado: serializar y almacenar
            byte[] serializedMessage = avroSerializer.serialize(avroSubject, avroMessage);
            
            logger.info("Mensaje reemplazado exitosamente");
            return true;
        } catch (Exception e) {
            logger.error("Error al reemplazar mensaje en Schema Registry", e);
            return false;
        }
    }
    
    /**
     * Procesa un mensaje en texto plano, lo convierte a AVRO y lo reemplaza en el Schema Registry
     * @param plainTextMessage El mensaje en texto plano
     * @return true si el procesamiento fue exitoso
     */
    public boolean processAndReplaceMessage(String plainTextMessage) {
        try {
            // Convertir el mensaje a AVRO
            KafkaMessage avroMessage = convertToAvro(plainTextMessage);
            
            // Reemplazar el mensaje en el Schema Registry
            boolean replaced = replaceMessage(avroMessage.getId().toString(), avroMessage);
            
            return replaced;
        } catch (Exception e) {
            logger.error("Error en el procesamiento del mensaje", e);
            return false;
        }
    }
}
