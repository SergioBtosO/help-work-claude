/**
 * CONFIGURACIÓN DE SPRING BOOT PARA MIGRACIÓN DE MULESOFT
 * 
 * Este archivo contiene todas las configuraciones necesarias para replicar
 * la funcionalidad de tu aplicación Mulesoft en Spring Boot.
 */

/**
 * 1. APPLICATION.YML - CONFIGURACIÓN PRINCIPAL
 */

# src/main/resources/application.yml
```yaml
spring:
  application:
    name: kafka-to-eventbridge-payments-connector
  
# Configuración de Kafka basada en Image 1
kafka:
  bootstrap-servers:
    - srhbmdlo21usb03.sys.mx.us.dev.corp:9092
    - srhbmdlo21usb03.sys.mx.us.dev.corp:9092
    - srhbmdlo21usb03.sys.mx.us.dev.corp:9092
  security:
    username: middle
    password: middleelkk_jaas
  consumer:
    group-id: HZ.PAYMENTSGDC.AVRO.CONSUMERTest13
    topic-pattern: SBNA.00002517.MIP_INS_HIST_EJ.MODIFY.AVRO
    consumer-amount: 1
    max-polling-interval: 6000
    request-timeout: 30
    default-record-limit: 500
    default-listener-poll-timeout: 100
    retry-backoff-timeout: 100
    reconnection:
      frequency: 6000
      attempts: 2
  schema-registry:
    username: middle
    password: middleelkk_jaas
    base-url: https://srhbmdlo22usb06.sys.mx.us.dev.corp:8081
    time-to-live: 3600
    reconnection:
      frequency: 6000
      attempts: 2

# Configuración de validación (Image 1)
validation:
  codesta2: "13"

# Configuración de AWS basada en Image 2 y 3
aws:
  default: aws1
  datetime-format: yyyyMMdd'T'HHmmss'Z'
  date-format: yyyyMMdd
  algorithm: AWS4-X509-RSA-SHA256
  content-type: application/json
  connection-timeout: 30000
  response-timeout: 30000
  reconnection:
    frequency: 6000
    attempts: 2
  
  # Configuración IAM (Image 2 y 3)
  iam:
    uri: /sessions
    method: POST
    service: rolesanywhere
    query-params: ""
    operation: ""
    hash: SHA-256
    session-name: assume_role_session
    
    # Configuración IAM AWS1 (Image 3)
    aws1:
      host: rolesanywhere.us-east-1.amazonaws.com
      profile-arn: arn:aws:rolesanywhere:us-east-1:335086986404:profile/081f2489-3e03-47e9-9f9c-7a03b80fe55b
      role-arn: arn:aws:iam::335086986404:role/dev-us-mbcrm-roles-anywhere
      trust-anchor-arn: arn:aws:rolesanywhere:us-east-1:335086986404:trust-anchor/d4b8a9cd-be4e-4d27-91fe-62d663c69987
      region: us-east-1
      certificate: /HzBackoffice/Horizon-Backoffice/certificates/certificate
      key: /HzBackoffice/Horizon-Backoffice/certificates/key
    
    # Configuración IAM AWS2 (Image 3)
    aws2:
      host: rolesanywhere.us-east-1.amazonaws.com
      profile-arn: arn:aws:rolesanywhere:us-east-1:461948903658:profile/98d6972a-7d17-4658-bddc-ac1b38025374
      role-arn: arn:aws:iam::461948903658:role/qa-us-mbcrm-roles-anywhere
      trust-anchor-arn: arn:aws:rolesanywhere:us-east-1:461948903658:trust-anchor/c8f815fc-ba89-4176-aa22-3acb68b722fb
      region: us-east-1
      certificate: /HzBackoffice/Horizon-Backoffice/certificates/certificate
      key: /HzBackoffice/Horizon-Backoffice/certificates/key
  
  # Configuración EventBridge (Image 2 y 3)
  eventbridge:
    service: events
    algorithm: AWS4-HMAC-SHA256
    content-type: application/x-amz-json-1.1
    method: POST
    uri: ""
    query-params: ""
    operation: ""
    amz-target: AWSEvents.PutEvents
    
    # Configuración EventBridge AWS1 (Image 3)
    aws1:
      host: events.us-east-1.amazonaws.com
      event-bus-name: dev-us-mb-sss
      region: us-east-1
    
    # Configuración EventBridge AWS2 (Image 3)
    aws2:
      host: events.us-east-1.amazonaws.com
      event-bus-name: dev-us-sss-mb
      region: us-east-1
    
    # Respuesta resultado (Image 2)
    result:
      correct: OK
      incorrect: KO

# Configuración de Redis basada en Image 4
redis:
  host: redis-11999.redisesb.sys.mx.us.pre.corp
  port: 11999
  password: R3dis_EsB_Pr3
  connection-timeout: 24000  # MILLISECONDS
  expiration-time: 3500      # SECONDS
  partition:
    identifier-aws1: HZMS-007-kafka-session-aws1
    identifier-aws2: HZMS-007-kafka-session-aws2

# Configuración de tópicos basada en Image 5
topics:
  pattern: SBNA.00002517.MIP_INS_HIST_EJ.MODIFY.AVRO
  aws:
    names:
      aws1: aws1
      aws2: aws2
  output:
    detail-type: Transfer_KO
    source: openbank.payments
    name: MIP_INS_HIST_EJ-transformed
```

/**
 * 2. CONFIGURACIÓN DE KAFKA
 */
// src/main/java/com/empresa/connector/config/KafkaConfig.java
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
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${kafka.bootstrap-servers}")
    private List<String> bootstrapServers;
    
    @Value("${kafka.security.username}")
    private String username;
    
    @Value("${kafka.security.password}")
    private String password;
    
    @Value("${kafka.consumer.group-id}")
    private String groupId;
    
    @Value("${kafka.consumer.max-polling-interval}")
    private Integer maxPollingInterval;
    
    @Value("${kafka.consumer.request-timeout}")
    private Integer requestTimeout;
    
    @Value("${kafka.consumer.default-record-limit}")
    private Integer defaultRecordLimit;
    
    @Value("${kafka.consumer.retry-backoff-timeout}")
    private Integer retryBackoffTimeout;
    
    @Value("${kafka.consumer.reconnection.attempts}")
    private Integer reconnectionAttempts;
    
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", bootstrapServers));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollingInterval);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout * 1000);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, defaultRecordLimit);

        // Configuración de JAAS para autenticación
        if (username != null && !username.isEmpty() && password != null && !password.isEmpty()) {
            String jaasConfig = "org.apache.kafka.common.security.plain.PlainLoginModule required "
                    + "username=\"" + username + "\" "
                    + "password=\"" + password + "\";";
            props.put("sasl.jaas.config", jaasConfig);
            props.put("security.protocol", "SASL_PLAINTEXT");
            props.put("sasl.mechanism", "PLAIN");
        }

        return new DefaultKafkaConsumerFactory<>(props);
    }
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        
        // Configuración de error handler con backoff
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                new FixedBackOff(retryBackoffTimeout, reconnectionAttempts.longValue()));
        factory.setCommonErrorHandler(errorHandler);
        
        // Configuración de concurrencia basada en el número de consumidores
        factory.setConcurrency(1); // Basado en consumer-amount: 1 en la Image 1
        
        // Modo de ack manual
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        
        return factory;
    }
}

/**
 * 3. CONFIGURACIÓN DE AWS
 */
// src/main/java/com/empresa/connector/config/AwsConfig.java
package com.empresa.connector.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;

import java.time.Duration;

@Configuration
public class AwsConfig {

    @Value("${aws.connection-timeout}")
    private Integer connectionTimeout;

    @Value("${aws.response-timeout}")
    private Integer responseTimeout;

    @Value("${aws.eventbridge.aws1.region}")
    private String aws1Region;

    @Bean
    public SdkHttpClient httpClient() {
        return ApacheHttpClient.builder()
                .connectionTimeout(Duration.ofMillis(connectionTimeout))
                .socketTimeout(Duration.ofMillis(responseTimeout))
                .build();
    }

    @Bean(name = "eventBridgeClientAws1")
    public EventBridgeClient eventBridgeClientAws1(
            SdkHttpClient httpClient, 
            @Value("${aws.iam.aws1.region}") String region) {
        return EventBridgeClient.builder()
                .region(Region.of(region))
                .httpClient(httpClient)
                .build();
    }

    @Bean(name = "eventBridgeClientAws2")
    public EventBridgeClient eventBridgeClientAws2(
            SdkHttpClient httpClient, 
            @Value("${aws.iam.aws2.region}") String region) {
        return EventBridgeClient.builder()
                .region(Region.of(region))
                .httpClient(httpClient)
                .build();
    }
}

/**
 * 4. CONFIGURACIÓN DE REDIS
 */
// src/main/java/com/empresa/connector/config/RedisConfig.java
package com.empresa.connector.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfig {

    @Value("${redis.host}")
    private String redisHost;

    @Value("${redis.port}")
    private int redisPort;

    @Value("${redis.password}")
    private String redisPassword;

    @Value("${redis.connection-timeout}")
    private int connectionTimeout;

    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        RedisStandaloneConfiguration redisConfig = new RedisStandaloneConfiguration();
        redisConfig.setHostName(redisHost);
        redisConfig.setPort(redisPort);
        redisConfig.setPassword(redisPassword);

        JedisConnectionFactory jedisConnectionFactory = new JedisConnectionFactory(redisConfig);
        jedisConnectionFactory.getPoolConfig().setMaxWait(Duration.ofMillis(connectionTimeout));
        
        return jedisConnectionFactory;
    }

    @Bean
    public RedisTemplate<String, String> redisTemplate() {
        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(redisConnectionFactory());
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setHashValueSerializer(new StringRedisSerializer());
        return template;
    }
}

/**
 * 5. CLASE DE PROPIEDADES PARA AWS IAM
 */
// src/main/java/com/empresa/connector/config/properties/AwsIamProperties.java
package com.empresa.connector.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "aws.iam")
public class AwsIamProperties {
    
    private String uri;
    private String method;
    private String service;
    private String queryParams;
    private String operation;
    private String hash;
    private String sessionName;
    
    private AwsInstanceProperties aws1;
    private AwsInstanceProperties aws2;
    
    @Data
    public static class AwsInstanceProperties {
        private String host;
        private String profileArn;
        private String roleArn;
        private String trustAnchorArn;
        private String region;
        private String certificate;
        private String key;
    }
}

/**
 * 6. CLASE DE PROPIEDADES PARA AWS EVENTBRIDGE
 */
// src/main/java/com/empresa/connector/config/properties/EventBridgeProperties.java
package com.empresa.connector.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "aws.eventbridge")
public class EventBridgeProperties {
    
    private String service;
    private String algorithm;
    private String contentType;
    private String method;
    private String uri;
    private String queryParams;
    private String operation;
    private String amzTarget;
    
    private EventBridgeInstanceProperties aws1;
    private EventBridgeInstanceProperties aws2;
    private ResultProperties result;
    
    @Data
    public static class EventBridgeInstanceProperties {
        private String host;
        private String eventBusName;
        private String region;
    }
    
    @Data
    public static class ResultProperties {
        private String correct;
        private String incorrect;
    }
}

/**
 * 7. CLASE DE PROPIEDADES PARA REDIS
 */
// src/main/java/com/empresa/connector/config/properties/RedisProperties.java
package com.empresa.connector.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "redis.partition")
public class RedisPartitionProperties {
    
    private String identifierAws1;
    private String identifierAws2;
}

/**
 * 8. IMPLEMENTACIÓN DE SERVICIO PARA IAM AWS
 */
// src/main/java/com/empresa/connector/service/AwsIamService.java
package com.empresa.connector.service;

import com.empresa.connector.config.properties.AwsIamProperties;
import com.empresa.connector.model.AwsCredentials;
import com.empresa.connector.util.CertificateLoader;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class AwsIamService {

    private final AwsIamProperties iamProperties;
    private final RedisService redisService;
    private final CertificateLoader certificateLoader;
    
    public AwsCredentials getAwsCredentialsForAws1() {
        return getAwsCredentials(iamProperties.getAws1(), "aws1");
    }
    
    public AwsCredentials getAwsCredentialsForAws2() {
        return getAwsCredentials(iamProperties.getAws2(), "aws2");
    }
    
    private AwsCredentials getAwsCredentials(AwsIamProperties.AwsInstanceProperties properties, String awsKey) {
        // Verificar si ya hay credenciales en Redis
        String cacheKey = String.format("aws:credentials:%s", awsKey);
        String cachedCredentials = redisService.getCache(cacheKey);
        
        if (cachedCredentials != null && !cachedCredentials.isEmpty()) {
            log.info("Recuperando credenciales de Redis para {}", awsKey);
            try {
                // Deserializar de JSON
                return AwsCredentials.fromJson(cachedCredentials);
            } catch (Exception e) {
                log.warn("Error al deserializar credenciales de Redis: {}", e.getMessage());
                // Continuar con obtención de nuevas credenciales
            }
        }
        
        try {
            // Cargar certificados
            X509Certificate certificate = certificateLoader.loadCertificate(properties.getCertificate());
            PrivateKey privateKey = certificateLoader.loadPrivateKey(properties.getKey());
            
            // Preparar la solicitud para el servicio IAM RolesAnywhere
            Map<String, String> headers = new HashMap<>();
            headers.put("X-Amz-X509", certificate.getEncoded().toString());
            
            // Construir el cuerpo de la solicitud
            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("profileArn", properties.getProfileArn());
            requestBody.put("roleArn", properties.getRoleArn());
            requestBody.put("trustAnchorArn", properties.getTrustAnchorArn());
            
            // Aquí irá la implementación real de la llamada al servicio AWS RolesAnywhere
            // Para esta ejemplificación, generamos unas credenciales de ejemplo
            
            AwsCredentials credentials = AwsCredentials.builder()
                    .accessKeyId("ASIA" + generateRandomString(16))
                    .secretAccessKey(generateRandomString(40))
                    .sessionToken(generateRandomString(100))
                    .region(properties.getRegion())
                    .build();
            
            // Guardar en Redis
            redisService.setCache(cacheKey, credentials.toJson(), 3500); // 3500 segundos (de tu configuración)
            
            return credentials;
        } catch (Exception e) {
            log.error("Error al obtener credenciales de AWS para {}: {}", awsKey, e.getMessage(), e);
            throw new RuntimeException("Error al obtener credenciales de AWS", e);
        }
    }
    
    private String generateRandomString(int length) {
        // Método auxiliar para generar string aleatorio
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            int index = (int) (chars.length() * Math.random());
            sb.append(chars.charAt(index));
        }
        return sb.toString();
    }
}

/**
 * 9. IMPLEMENTACIÓN DEL SERVICIO EVENTBRIDGE
 */
// src/main/java/com/empresa/connector/service/EventBridgeService.java
package com.empresa.connector.service;

import com.empresa.connector.config.properties.EventBridgeProperties;
import com.empresa.connector.model.AwsCredentials;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse;

import javax.annotation.Resource;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventBridgeService {

    private final EventBridgeProperties eventBridgeProperties;
    private final AwsIamService awsIamService;
    private final ObjectMapper objectMapper;
    
    @Resource(name = "eventBridgeClientAws1")
    private EventBridgeClient eventBridgeClientAws1;
    
    @Resource(name = "eventBridgeClientAws2")
    private EventBridgeClient eventBridgeClientAws2;
    
    public String sendToEventBridgeWithAws1Config(String message) {
        try {
            AwsCredentials credentials = awsIamService.getAwsCredentialsForAws1();
            
            EventBridgeProperties.EventBridgeInstanceProperties aws1Props = eventBridgeProperties.getAws1();
            
            // Preparar la entrada de eventos
            PutEventsRequestEntry entry = PutEventsRequestEntry.builder()
                    .eventBusName(aws1Props.getEventBusName())
                    .source(getSourceFromConfig())
                    .detailType(getDetailTypeFromConfig())
                    .detail(message)
                    .build();
            
            // Construir la solicitud
            PutEventsRequest request = PutEventsRequest.builder()
                    .entries(entry)
                    .build();
            
            // Enviar a EventBridge
            PutEventsResponse response = eventBridgeClientAws1.putEvents(request);
            
            // Verificar respuesta
            if (response.failedEntryCount() > 0) {
                log.error("Error al enviar eventos a AWS1 EventBridge: {} entradas fallidas", 
                        response.failedEntryCount());
                return eventBridgeProperties.getResult().getIncorrect();
            } else {
                log.info("Eventos enviados correctamente a AWS1 EventBridge");
                return eventBridgeProperties.getResult().getCorrect();
            }
            
        } catch (Exception e) {
            log.error("Error al enviar a AWS1 EventBridge: {}", e.getMessage(), e);
            return eventBridgeProperties.getResult().getIncorrect();
        }
    }
    
    public String sendToEventBridgeWithAws2Config(String message) {
        try {
            AwsCredentials credentials = awsIamService.getAwsCredentialsForAws2();
            
            EventBridgeProperties.EventBridgeInstanceProperties aws2Props = eventBridgeProperties.getAws2();
            
            // Preparar la entrada de eventos
            PutEventsRequestEntry entry = PutEventsRequestEntry.builder()
                    .eventBusName(aws2Props.getEventBusName())
                    .source(getSourceFromConfig())
                    .detailType(getDetailTypeFromConfig())
                    .detail(message)
                    .build();
            
            // Construir la solicitud
            PutEventsRequest request = PutEventsRequest.builder()
                    .entries(entry)
                    .build();
            
            // Enviar a EventBridge
            PutEventsResponse response = eventBridgeClientAws2.putEvents(request);
            
            // Verificar respuesta
            if (response.failedEntryCount() > 0) {
                log.error("Error al enviar eventos a AWS2 EventBridge: {} entradas fallidas", 
                        response.failedEntryCount());
                return eventBridgeProperties.getResult().getIncorrect();
            } else {
                log.info("Eventos enviados correctamente a AWS2 EventBridge");
                return eventBridgeProperties.getResult().getCorrect();
            }
            
        } catch (Exception e) {
            log.error("Error al enviar a AWS2 EventBridge: {}", e.getMessage(), e);
            return eventBridgeProperties.getResult().getIncorrect();
        }
    }
    
    private String getSourceFromConfig() {
        return "openbank.payments"; // Valor obtenido de Image 5
    }
    
    private String getDetailTypeFromConfig() {
        return "Transfer_KO"; // Valor obtenido de Image 5
    }
}

/**
 * 10. IMPLEMENTACIÓN DEL SERVICIO REDIS
 */
// src/main/java/com/empresa/connector/service/RedisService.java
package com.empresa.connector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class RedisService {

    private final RedisTemplate<String, String> redisTemplate;
    
    @Value("${redis.expiration-time}")
    private long defaultExpirationTime;
    
    public String getCache(String key) {
        try {
            log.info("START GET Redis: {}", key);
            String value = redisTemplate.opsForValue().get(key);
            
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
    
    public void setCache(String key, String value, long timeout) {
        try {
            log.info("START SET Redis: {}", key);
            redisTemplate.opsForValue().set(key, value, timeout, TimeUnit.SECONDS);
            log.info("END SET Redis");
        } catch (Exception e) {
            log.error("Error en SET Redis: {}", e.getMessage(), e);
            log.info("END SET Redis (con error)");
        }
    }
    
    public void setCache(String key, String value) {
        setCache(key, value, defaultExpirationTime);
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
 * 11. MODELO AWS CREDENTIALS
 */
// src/main/java/com/empresa/connector/model/AwsCredentials.java
package com.empresa.connector.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AwsCredentials {
    
    @JsonProperty("AccessKeyId")
    private String accessKeyId;
    
    @JsonProperty("SecretAccessKey")
    private String secretAccessKey;
    
    @JsonProperty("SessionToken")
    private String sessionToken;
    
    @JsonProperty("Region")
    private String region;
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    /**
     * Convierte las credenciales de AWS a formato JSON
     * @return String con el formato JSON de las credenciales
     * @throws JsonProcessingException si ocurre un error al procesar el JSON
     */
    public String toJson() throws JsonProcessingException {
        return objectMapper.writeValueAsString(this);
    }
    
    /**
     * Crea un objeto AwsCredentials a partir de un String en formato JSON
     * @param json String con el formato JSON de las credenciales
     * @return Objeto AwsCredentials con los datos del JSON
     * @throws JsonProcessingException si ocurre un error al procesar el JSON
     */
    public static AwsCredentials fromJson(String json) throws JsonProcessingException {
        return objectMapper.readValue(json, AwsCredentials.class);
    }
    
    /**
     * Verifica si las credenciales son temporales
     * @return true si las credenciales son temporales (tienen sessionToken), false en caso contrario
     */
    public boolean isTemporary() {
        return sessionToken != null && !sessionToken.isEmpty();
    }
}