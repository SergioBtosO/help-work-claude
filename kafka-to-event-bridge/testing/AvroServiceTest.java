package com.example.kafka.service;

import com.example.kafka.avro.KafkaMessage;
import com.example.kafka.model.PlainTextMessage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Servicio que serializa mensajes usando Avro y luego los convierte a JSON
 */
@Service
public class AvroJsonService {
    private static final Logger logger = LoggerFactory.getLogger(AvroJsonService.class);
    
    private final SchemaRegistryClient schemaRegistryClient;
    private KafkaAvroSerializer avroSerializer;
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;
    
    @Value("${schema.subject:kafka-message-value}")
    private String schemaSubject;
    
    @Autowired
    public AvroJsonService(SchemaRegistryClient schemaRegistryClient) {
        this.schemaRegistryClient = schemaRegistryClient;
        
        // La inicialización del serializador se hará en el método init
    }
    
    @PostConstruct
    public void initializeSerializer() {
        // Configurar el serializador Avro
        Map<String, Object> config = new HashMap<>();
        config.put("schema.registry.url", schemaRegistryUrl);
        config.put("auto.register.schemas", true);
        
        this.avroSerializer = new KafkaAvroSerializer(schemaRegistryClient);
        this.avroSerializer.configure(config, false);
        
        logger.info("AvroJsonService inicializado para subject: {}", schemaSubject);
    }
    
    /**
     * Crea un mensaje Avro a partir de texto plano
     */
    public KafkaMessage createAvroMessage(String plainText) {
        return KafkaMessage.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setContent(plainText)
                .setTimestamp(System.currentTimeMillis())
                .setSource("kafka-consumer-service")
                .setProperties(new HashMap<>())
                .build();
    }
    
    /**
     * Crea un mensaje Avro a partir de un PlainTextMessage
     */
    public KafkaMessage createAvroMessage(PlainTextMessage message) {
        Map<CharSequence, CharSequence> avroProperties = new HashMap<>();
        if (message.getProperties() != null) {
            message.getProperties().forEach((key, value) -> 
                avroProperties.put(key, value != null ? value : ""));
        }
        
        return KafkaMessage.newBuilder()
                .setId(message.getId() != null ? message.getId() : UUID.randomUUID().toString())
                .setContent(message.getContent())
                .setTimestamp(message.getTimestamp() != null ? 
                      message.getTimestamp() : System.currentTimeMillis())
                .setSource(message.getSource() != null ? message.getSource() : "kafka-consumer-service")
                .setProperties(avroProperties)
                .build();
    }
    
    /**
     * Serializa un mensaje Avro y lo registra en Schema Registry
     */
    public byte[] serializeAvro(KafkaMessage avroMessage) {
        try {
            return avroSerializer.serialize(schemaSubject, avroMessage);
        } catch (Exception e) {
            logger.error("Error serializando a Avro", e);
            throw new RuntimeException("Error de serialización Avro", e);
        }
    }
    
    /**
     * Convierte un mensaje Avro a formato JSON
     */
    public JsonNode avroToJson(KafkaMessage avroMessage) {
        try {
            // Obtener el esquema
            Schema schema = avroMessage.getSchema();
            
            // Serializar a formato JSON
            ByteArrayOutputStream jsonOutputStream = new ByteArrayOutputStream();
            JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(schema, jsonOutputStream, false);
            
            GenericDatumWriter<KafkaMessage> writer = new GenericDatumWriter<>(schema);
            writer.write(avroMessage, jsonEncoder);
            jsonEncoder.flush();
            
            // Convertir a JsonNode
            return objectMapper.readTree(jsonOutputStream.toString());
        } catch (Exception e) {
            logger.error("Error convirtiendo Avro a JSON", e);
            throw new RuntimeException("Error en conversión Avro-JSON", e);
        }
    }
    
    /**
     * Procesa un mensaje de texto, serializándolo con Avro y convirtiéndolo a JSON
     */
    public JsonNode processMessage(String plainText) {
        try {
            logger.info("Procesando mensaje con Avro y JSON: {}", plainText);
            
            // Crear mensaje Avro
            KafkaMessage avroMessage = createAvroMessage(plainText);
            
            // Serializar con Avro (registra en Schema Registry)
            byte[] avroData = serializeAvro(avroMessage);
            logger.info("Mensaje serializado a Avro. Tamaño: {} bytes", avroData.length);
            
            // Convertir a JSON
            JsonNode jsonResult = avroToJson(avroMessage);
            logger.info("Conversión a JSON completada exitosamente");
            
            return jsonResult;
        } catch (Exception e) {
            logger.error("Error en el procesamiento del mensaje", e);
            throw new RuntimeException("Error procesando mensaje", e);
        }
    }
    
    /**
     * Procesa un objeto PlainTextMessage, serializándolo con Avro y convirtiéndolo a JSON
     */
    public JsonNode processMessage(PlainTextMessage message) {
        try {
            logger.info("Procesando PlainTextMessage con Avro y JSON: {}", message.getContent());
            
            // Crear mensaje Avro
            KafkaMessage avroMessage = createAvroMessage(message);
            
            // Serializar con Avro (registra en Schema Registry)
            byte[] avroData = serializeAvro(avroMessage);
            logger.info("Mensaje serializado a Avro. Tamaño: {} bytes", avroData.length);
            
            // Convertir a JSON
            JsonNode jsonResult = avroToJson(avroMessage);
            logger.info("Conversión a JSON completada exitosamente");
            
            return jsonResult;
        } catch (Exception e) {
            logger.error("Error en el procesamiento del mensaje", e);
            throw new RuntimeException("Error procesando mensaje", e);
        }
    }
    
    /**
     * Obtiene la representación en String de un JsonNode
     */
    public String getJsonString(JsonNode jsonNode) {
        try {
            return objectMapper.writeValueAsString(jsonNode);
        } catch (Exception e) {
            logger.error("Error serializando JSON a String", e);
            return null;
        }
    }
}
