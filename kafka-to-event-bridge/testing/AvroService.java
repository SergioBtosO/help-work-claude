package com.example.kafka.service;

import com.example.kafka.avro.KafkaMessage;
import com.example.kafka.model.PlainTextMessage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
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
    private final KafkaAvroSerializer avroSerializer;
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Value("${schema.subject:kafka-message-value}")
    private String schemaSubject;
    
    @Autowired
    public AvroJsonService(SchemaRegistryClient schemaRegistryClient) {
        this.schemaRegistryClient = schemaRegistryClient;
        
        // Configurar el serializador Avro
        Map<String, Object> config = new HashMap<>();
        config.put("schema.registry.url", "http://localhost:8081");
        config.put("auto.register.schemas", true);
        
        this.avroSerializer = new KafkaAvroSerializer(schemaRegistryClient);
        this.avroSerializer.configure(config, false);
    }
    
    /**
     * Convierte un mensaje de texto plano a un objeto Avro
     * @param plainText El mensaje como texto plano
     * @return El objeto Avro
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
     * Convierte un objeto PlainTextMessage a un objeto Avro
     * @param message El objeto mensaje
     * @return El objeto Avro
     */
    public KafkaMessage createAvroMessage(PlainTextMessage message) {
        // Convertir propiedades a formato compatible con AVRO
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
     * Serializa un objeto Avro a formato binario
     * @param avroMessage El objeto Avro a serializar
     * @return Los bytes serializados
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
     * Convierte un objeto Avro a formato JSON
     * @param avroMessage El objeto Avro
     * @return El nodo JSON resultante
     */
    public JsonNode avroToJson(KafkaMessage avroMessage) {
        try {
            // Obtener el esquema
            Schema schema = avroMessage.getSchema();
            
            // Serializar a Avro binario primero
            byte[] avroData = serializeAvro(avroMessage);
            
            // Convertir de Avro binario a JSON
            ByteArrayOutputStream jsonOutputStream = new ByteArrayOutputStream();
            JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(schema, jsonOutputStream);
            
            // Usar SpecificDatumReader para objetos Avro generados
            SpecificDatumReader<KafkaMessage> reader = new SpecificDatumReader<>(schema);
            BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(avroData, null);
            KafkaMessage result = reader.read(null, binaryDecoder);
            
            GenericDatumWriter<KafkaMessage> writer = new GenericDatumWriter<>(schema);
            writer.write(result, jsonEncoder);
            jsonEncoder.flush();
            
            // Convertir la salida JSON a JsonNode
            return objectMapper.readTree(jsonOutputStream.toString());
        } catch (Exception e) {
            logger.error("Error convirtiendo Avro a JSON", e);
            throw new RuntimeException("Error en conversión Avro-JSON", e);
        }
    }
    
    /**
     * Procesa un mensaje, serializándolo con Avro y luego convirtiéndolo a JSON
     * @param plainText El mensaje como texto plano
     * @return El nodo JSON resultante
     */
    public JsonNode processMessage(String plainText) {
        try {
            logger.info("Procesando mensaje con Avro y JSON: {}", plainText);
            
            // Crear mensaje Avro
            KafkaMessage avroMessage = createAvroMessage(plainText);
            
            // Serializar con Avro (para verificar y registrar el esquema)
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
     * Procesa un objeto PlainTextMessage, serializándolo con Avro y luego convirtiéndolo a JSON
     * @param message El objeto mensaje
     * @return El nodo JSON resultante
     */
    public JsonNode processMessage(PlainTextMessage message) {
        try {
            logger.info("Procesando PlainTextMessage con Avro y JSON: {}", message.getContent());
            
            // Crear mensaje Avro
            KafkaMessage avroMessage = createAvroMessage(message);
            
            // Serializar con Avro (para verificar y registrar el esquema)
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
     * Obtiene el resultado JSON como String
     * @param jsonNode El nodo JSON
     * @return La representación en String del JSON
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
