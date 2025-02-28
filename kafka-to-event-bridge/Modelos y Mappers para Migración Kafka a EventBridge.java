/**
 * MODELOS Y MAPPERS PARA LA MIGRACIÓN DE KAFKA A EVENTBRIDGE
 */

/**
 * 1. MODELO DE DATOS KAFKA (INPUT)
 */
package com.empresa.connector.model.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KafkaPaymentMessage {
    
    @JsonProperty("G6181_IDEMPR")
    private String idempr;
    
    @JsonProperty("G6181_CCENCONT")
    private String ccencont;
    
    @JsonProperty("G6181_TIPOPRD")
    private String tipoprd;
    
    @JsonProperty("G6181_CCONTRAT")
    private String ccontrat;
    
    @JsonProperty("G6181_NUMORD")
    private String numord;
    
    @JsonProperty("G6181_JNUMDET")
    private String jnumdet;
    
    @JsonProperty("G6181_FECHAEJE")
    private String fechaeje;
    
    @JsonProperty("G6181_CODESTA2")
    private String codesta2;
    
    // Métodos auxiliares para validaciones específicas
    
    public boolean isValidCodesta2() {
        return "13".equals(codesta2);
    }
    
    public String buildOperationId() {
        // Ejemplo basado en la imagen 2
        return String.format("%s-%s-%s-%s",
                ccencont + numord + jnumdet,
                "01001",
                "00000",
                fechaeje);
    }
}

/**
 * 2. MODELO DE DATOS EVENTBRIDGE (OUTPUT)
 */
package com.empresa.connector.model.eventbridge;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventBridgePayload {
    
    @JsonProperty("operationId")
    private String operationId;
}

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventBridgeDetail {
    
    @JsonProperty("payload")
    private EventBridgePayload payload;
}

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventBridgeMessage {
    
    @JsonProperty("detail-type")
    private String detailType;
    
    @JsonProperty("source")
    private String source;
    
    @JsonProperty("detail")
    private EventBridgeDetail detail;
}

/**
 * 3. MAPPERS CON MAPSTRUCT
 */
package com.empresa.connector.mapper;

import com.empresa.connector.model.kafka.KafkaPaymentMessage;
import com.empresa.connector.model.eventbridge.EventBridgeMessage;
import com.empresa.connector.model.eventbridge.EventBridgeDetail;
import com.empresa.connector.model.eventbridge.EventBridgePayload;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Named;
import org.springframework.beans.factory.annotation.Value;

@Mapper(componentModel = "spring")
public interface KafkaToEventBridgeMapper {
    
    @Value("${eventbridge.source}")
    String defaultSource = "openbank.payments";
    
    @Value("${eventbridge.detail-type}")
    String defaultDetailType = "Transfer_KO";
    
    @Mapping(target = "detailType", constant = "Transfer_KO")
    @Mapping(target = "source", constant = "openbank.payments")
    @Mapping(target = "detail", source = "kafkaMessage", qualifiedByName = "toEventBridgeDetail")
    EventBridgeMessage toEventBridgeMessage(KafkaPaymentMessage kafkaMessage);
    
    @Named("toEventBridgeDetail")
    default EventBridgeDetail toEventBridgeDetail(KafkaPaymentMessage kafkaMessage) {
        return EventBridgeDetail.builder()
                .payload(EventBridgePayload.builder()
                        .operationId(kafkaMessage.buildOperationId())
                        .build())
                .build();
    }
}

/**
 * 4. CONFIGURACIÓN DE MAPSTRUCT
 */
package com.empresa.connector.config;

import org.mapstruct.InjectionStrategy;
import org.mapstruct.MapperConfig;
import org.mapstruct.ReportingPolicy;

@MapperConfig(
    componentModel = "spring",
    injectionStrategy = InjectionStrategy.CONSTRUCTOR,
    unmappedTargetPolicy = ReportingPolicy.IGNORE
)
public interface MapStructConfig {
}

/**
 * 5. SERVICIO DE TRANSFORMACIÓN
 */
package com.empresa.connector.service;

import com.empresa.connector.mapper.KafkaToEventBridgeMapper;
import com.empresa.connector.model.kafka.KafkaPaymentMessage;
import com.empresa.connector.model.eventbridge.EventBridgeMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransformationService {
    
    private final KafkaToEventBridgeMapper mapper;
    private final ObjectMapper objectMapper;
    
    /**
     * Transforma un mensaje Kafka en formato JSON a un objeto KafkaPaymentMessage
     */
    public KafkaPaymentMessage parseKafkaMessage(String kafkaMessageJson) {
        try {
            return objectMapper.readValue(kafkaMessageJson, KafkaPaymentMessage.class);
        } catch (Exception e) {
            log.error("Error al parsear mensaje Kafka: {}", e.getMessage());
            throw new RuntimeException("Error al parsear mensaje Kafka", e);
        }
    }
    
    /**
     * Transforma un objeto KafkaPaymentMessage a un EventBridgeMessage
     */
    public EventBridgeMessage transformToEventBridgeMessage(KafkaPaymentMessage kafkaMessage) {
        return mapper.toEventBridgeMessage(kafkaMessage);
    }
    
    /**
     * Convierte un EventBridgeMessage a formato JSON
     */
    public String toEventBridgeJson(EventBridgeMessage eventBridgeMessage) {
        try {
            return objectMapper.writeValueAsString(eventBridgeMessage);
        } catch (Exception e) {
            log.error("Error al convertir EventBridgeMessage a JSON: {}", e.getMessage());
            throw new RuntimeException("Error al convertir EventBridgeMessage a JSON", e);
        }
    }
    
    /**
     * Proceso completo: convierte un mensaje Kafka en JSON a un mensaje EventBridge en JSON
     */
    public String transformKafkaToEventBridge(String kafkaMessageJson) {
        KafkaPaymentMessage kafkaMessage = parseKafkaMessage(kafkaMessageJson);
        EventBridgeMessage eventBridgeMessage = transformToEventBridgeMessage(kafkaMessage);
        return toEventBridgeJson(eventBridgeMessage);
    }
}

/**
 * 6. INTEGRACIÓN CON EL HANDLER
 */
package com.empresa.connector.handler;

import com.empresa.connector.model.kafka.KafkaPaymentMessage;
import com.empresa.connector.orchestrator.KafkaMessageToEventBridgeOrchestrator;
import com.empresa.connector.service.TransformationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaToEventBridgeHandler {

    private final KafkaMessageToEventBridgeOrchestrator orchestrator;
    private final TransformationService transformationService;

    public void processEvent(String payload, String topic, Integer partition, Long offset) {
        try {
            // Set Variable: Save originalPayload
            String originalPayload = payload;
            
            // Transform Message: Parse Kafka message
            KafkaPaymentMessage kafkaMessage = transformationService.parseKafkaMessage(payload);
            
            // Log mensaje parseado
            log.debug("Mensaje Kafka parseado: {}", kafkaMessage);
            
            // Verificar CODESTA2
            if (kafkaMessage.isValidCodesta2()) {
                // Transformar a EventBridge y enviar
                String eventBridgeJson = transformationService.transformKafkaToEventBridge(payload);
                
                // Flow Reference: Send Kafka message to EventBridge
                orchestrator.processAndSendToEventBridge(eventBridgeJson, originalPayload);
            } else {
                log.warn("CODESTA2 no válido: {}. No se procesará el mensaje.", kafkaMessage.getCodesta2());
            }
            
        } catch (Exception e) {
            log.error("Error en el procesamiento del evento: {}", e.getMessage(), e);
            throw new RuntimeException("Error procesando mensaje Kafka", e);
        }
    }
}

/**
 * 7. DEPENDENCIAS MAVEN (agregado al pom.xml)
 */

/*
<dependencies>
    <!-- MapStruct -->
    <dependency>
        <groupId>org.mapstruct</groupId>
        <artifactId>mapstruct</artifactId>
        <version>1.5.5.Final</version>
    </dependency>
    <dependency>
        <groupId>org.mapstruct</groupId>
        <artifactId>mapstruct-processor</artifactId>
        <version>1.5.5.Final</version>
        <scope>provided</scope>
    </dependency>
    
    <!-- Lombok (combinación con MapStruct) -->
    <dependency>
        <groupId>org.projectlombok</groupId>
        <artifactId>lombok</artifactId>
        <version>1.18.30</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.projectlombok</groupId>
        <artifactId>lombok-mapstruct-binding</artifactId>
        <version>0.2.0</version>
    </dependency>
</dependencies>
*/

/**
 * 8. PROPIEDADES DE CONFIGURACIÓN (application.yml)
 */

/*
# Configuración de EventBridge
eventbridge:
  source: openbank.payments
  detail-type: Transfer_KO
*/