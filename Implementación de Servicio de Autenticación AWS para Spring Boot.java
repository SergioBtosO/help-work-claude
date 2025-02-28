/**
 * IMPLEMENTACIÓN DE SERVICIO DE AUTENTICACIÓN AWS PARA SPRING BOOT
 * 
 * Este código implementa la funcionalidad de autenticación AWS según 
 * las imágenes compartidas del código Mulesoft.
 */

/**
 * 1. CLASE DE SERVICIO PRINCIPAL DE AUTENTICACIÓN AWS
 */
package com.empresa.connector.service;

import com.empresa.connector.config.properties.AwsProperties;
import com.empresa.connector.model.AwsCredentials;
import com.empresa.connector.util.AwsSigner;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class AwsAuthService {

    private final AwsProperties awsProperties;
    private final AwsSigner awsSigner;
    private final RestTemplate restTemplate;
    
    /**
     * Genera los encabezados de autenticación para AWS
     */
    public HttpHeaders generateSecureAwsHeaders(String method, 
                                             String region, 
                                             String service, 
                                             String operation, 
                                             String uri, 
                                             String queryParams, 
                                             String requestBody,
                                             AwsCredentials credentials) {
        try {
            // Obtener fecha actual en formato AWS
            ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
            String awsDate = now.format(DateTimeFormatter.ofPattern(awsProperties.getDatetimeFormat()));
            String dateStamp = now.format(DateTimeFormatter.ofPattern(awsProperties.getDateFormat()));
            
            // Calcular hash del payload
            String payloadHash = awsSigner.hashContent(requestBody);
            
            // Crear mapa de encabezados básicos
            Map<String, String> headerMap = new HashMap<>();
            headerMap.put("host", String.format("%s.%s.amazonaws.com", service, region));
            headerMap.put("x-amz-content-sha256", payloadHash);
            headerMap.put("x-amz-date", awsDate);
            
            // Si hay target, agregarlo al encabezado
            if (operation != null && !operation.isEmpty()) {
                headerMap.put("x-amz-target", operation);
            }
            
            // Si hay token de seguridad, agregarlo al encabezado
            if (credentials.getSessionToken() != null && !credentials.getSessionToken().isEmpty()) {
                headerMap.put("x-amz-security-token", credentials.getSessionToken());
            }
            
            // Calcular la autorización AWS
            String authorization = awsSigner.computeAuthorizationHeader(
                awsDate, 
                dateStamp, 
                region, 
                service,
                method,
                uri,
                queryParams,
                headerMap,
                payloadHash,
                credentials);
                
            // Crear encabezados para la petición HTTP
            HttpHeaders headers = new HttpHeaders();
            headers.add("Authorization", authorization);
            headers.add("Content-Type", awsProperties.getContentType());
            
            // Agregar todos los encabezados del mapa
            headerMap.forEach(headers::add);
            
            return headers;
        } catch (Exception e) {
            log.error("Error generando encabezados AWS: {}", e.getMessage(), e);
            throw new RuntimeException("Error generando encabezados AWS", e);
        }
    }
    
    /**
     * Envía una petición autenticada a un servicio AWS
     */
    public <T> ResponseEntity<T> sendAuthenticatedRequest(String url, 
                                                     HttpMethod httpMethod, 
                                                     Object body, 
                                                     Class<T> responseType,
                                                     AwsCredentials credentials,
                                                     String region,
                                                     String service,
                                                     String operation) {
        try {
            // Extraer URI y query params
            String uri = extractUri(url);
            String queryParams = extractQueryParams(url);
            
            // Convertir body a String si no es null
            String requestBody = (body != null) ? convertBodyToString(body) : "";
            
            // Generar encabezados con firma AWS
            HttpHeaders headers = generateSecureAwsHeaders(
                httpMethod.name(), 
                region, 
                service, 
                operation, 
                uri, 
                queryParams, 
                requestBody,
                credentials);
            
            // Construir la entidad de la petición
            HttpEntity<?> requestEntity = new HttpEntity<>(body, headers);
            
            // Hacer la petición
            return restTemplate.exchange(url, httpMethod, requestEntity, responseType);
        } catch (Exception e) {
            log.error("Error enviando petición autenticada a AWS: {}", e.getMessage(), e);
            throw new RuntimeException("Error enviando petición autenticada a AWS", e);
        }
    }
    
    /**
     * Métodos auxiliares
     */
    private String extractUri(String url) {
        try {
            int queryStart = url.indexOf('?');
            if (queryStart == -1) {
                return url.substring(url.indexOf("/", 8)); // 8 para saltar "https://"
            } else {
                return url.substring(url.indexOf("/", 8), queryStart);
            }
        } catch (Exception e) {
            return "/";
        }
    }
    
    private String extractQueryParams(String url) {
        int queryStart = url.indexOf('?');
        if (queryStart == -1) {
            return "";
        } else {
            return url.substring(queryStart + 1);
        }
    }
    
    private String convertBodyToString(Object body) {
        if (body == null) {
            return "";
        } else if (body instanceof String) {
            return (String) body;
        } else {
            // Aquí podrías usar ObjectMapper para convertir a JSON
            return body.toString();
        }
    }
}

/**
 * 2. UTILIDAD AWS SIGNER - Implementa el algoritmo de firma AWS Signature V4
 */
package com.empresa.connector.util;

import com.empresa.connector.model.AwsCredentials;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
public class AwsSigner {

    // Constante para el algoritmo de firma
    private static final String HMAC_SHA256 = "HmacSHA256";
    private static final String EMPTY_STRING_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    
    /**
     * Calcula el hash SHA-256 de un contenido
     */
    public String hashContent(String content) {
        try {
            if (content == null || content.isEmpty()) {
                return EMPTY_STRING_SHA256;
            }
            
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(content.getBytes(StandardCharsets.UTF_8));
            return bytesToHex(digest).toLowerCase();
        } catch (Exception e) {
            log.error("Error calculando hash de contenido: {}", e.getMessage(), e);
            throw new RuntimeException("Error calculando hash de contenido", e);
        }
    }
    
    /**
     * Computa el encabezado de autorización AWS
     * Basado en las imágenes 1, 2 y 3 compartidas
     */
    public String computeAuthorizationHeader(String awsDate, 
                                           String dateStamp, 
                                           String region, 
                                           String service,
                                           String method,
                                           String uri,
                                           String queryString,
                                           Map<String, String> headers,
                                           String payloadHash,
                                           AwsCredentials credentials) {
        try {
            // Paso 1: Crear la petición canónica
            String canonicalRequest = createCanonicalRequest(method, uri, queryString, headers, payloadHash);
            
            // Paso 2: Crear la cadena de firma
            String stringToSign = createStringToSign(awsDate, dateStamp, region, service, canonicalRequest);
            
            // Paso 3: Calcular la clave de firma
            byte[] signingKey = getSignatureKey(credentials.getSecretAccessKey(), dateStamp, region, service);
            
            // Paso 4: Calcular la firma
            byte[] signature = hmacSha256(signingKey, stringToSign);
            String signatureHex = bytesToHex(signature).toLowerCase();
            
            // Construir el encabezado de autorización
            return String.format(
                "%s Credential=%s/%s/%s/%s/aws4_request, SignedHeaders=%s, Signature=%s",
                "AWS4-HMAC-SHA256",
                credentials.getAccessKeyId(),
                dateStamp,
                region,
                service,
                getSignedHeadersString(headers),
                signatureHex
            );
        } catch (Exception e) {
            log.error("Error computando encabezado de autorización: {}", e.getMessage(), e);
            throw new RuntimeException("Error computando encabezado de autorización", e);
        }
    }
    
    /**
     * Crea la petición canónica según el formato AWS
     * Basado en la imagen 3
     */
    private String createCanonicalRequest(String method, 
                                        String uri, 
                                        String queryString, 
                                        Map<String, String> headers,
                                        String payloadHash) {
        // Formato:
        // HTTPRequestMethod + '\n' +
        // CanonicalURI + '\n' +
        // CanonicalQueryString + '\n' +
        // CanonicalHeaders + '\n' +
        // SignedHeaders + '\n' +
        // HexEncode(Hash(RequestPayload))
        
        StringBuilder canonicalRequest = new StringBuilder();
        canonicalRequest.append(method).append('\n');
        canonicalRequest.append(uri == null || uri.isEmpty() ? "/" : uri).append('\n');
        canonicalRequest.append(canonicalQueryParameters(queryString)).append('\n');
        canonicalRequest.append(canonicalHeaders(headers)).append('\n');
        canonicalRequest.append(getSignedHeadersString(headers)).append('\n');
        canonicalRequest.append(payloadHash);
        
        return canonicalRequest.toString();
    }
    
    /**
     * Crea la cadena de firma (StringToSign)
     * Basado en la imagen 2
     */
    private String createStringToSign(String awsDate, 
                                    String dateStamp, 
                                    String region, 
                                    String service,
                                    String canonicalRequest) {
        // Formato:
        // Algorithm + '\n' +
        // RequestDateTime + '\n' +
        // CredentialScope + '\n' +
        // HashedCanonicalRequest
        
        String credentialScope = String.format("%s/%s/%s/aws4_request", dateStamp, region, service);
        String hashedCanonicalRequest = hashContent(canonicalRequest);
        
        return "AWS4-HMAC-SHA256" + '\n' + 
               awsDate + '\n' + 
               credentialScope + '\n' + 
               hashedCanonicalRequest;
    }
    
    /**
     * Obtiene la clave de firma
     * Basado en la imagen 2
     */
    private byte[] getSignatureKey(String secretKey, String dateStamp, String region, String service) {
        try {
            String keyPrefix = "AWS4" + secretKey;
            byte[] kDate = hmacSha256(keyPrefix.getBytes(StandardCharsets.UTF_8), dateStamp);
            byte[] kRegion = hmacSha256(kDate, region);
            byte[] kService = hmacSha256(kRegion, service);
            return hmacSha256(kService, "aws4_request");
        } catch (Exception e) {
            log.error("Error obteniendo clave de firma: {}", e.getMessage(), e);
            throw new RuntimeException("Error obteniendo clave de firma", e);
        }
    }
    
    /**
     * Implementación de HMAC-SHA256
     */
    private byte[] hmacSha256(byte[] key, String data) {
        try {
            Mac mac = Mac.getInstance(HMAC_SHA256);
            mac.init(new SecretKeySpec(key, HMAC_SHA256));
            return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.error("Error calculando HMAC-SHA256: {}", e.getMessage(), e);
            throw new RuntimeException("Error calculando HMAC-SHA256", e);
        }
    }
    
    /**
     * Convierte un array de bytes a su representación hexadecimal
     */
    private String bytesToHex(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (byte b : bytes) {
            result.append(String.format("%02x", b));
        }
        return result.toString();
    }
    
    /**
     * Crea los encabezados canónicos
     * Basado en la imagen 3
     */
    private String canonicalHeaders(Map<String, String> headers) {
        // Convertir las claves a minúsculas y ordenarlas
        List<String> sortedKeys = new ArrayList<>(headers.keySet());
        sortedKeys.sort(String::compareToIgnoreCase);
        
        StringBuilder result = new StringBuilder();
        for (String key : sortedKeys) {
            result.append(key.toLowerCase().trim())
                  .append(':')
                  .append(headers.get(key).trim())
                  .append('\n');
        }
        
        return result.toString();
    }
    
    /**
     * Obtiene la cadena de encabezados firmados
     */
    private String getSignedHeadersString(Map<String, String> headers) {
        return headers.keySet().stream()
                .map(key -> key.toLowerCase().trim())
                .sorted()
                .collect(Collectors.joining(";"));
    }
    
    /**
     * Crea los parámetros de consulta canónicos
     */
    private String canonicalQueryParameters(String queryString) {
        if (queryString == null || queryString.isEmpty()) {
            return "";
        }
        
        // Parsear los parámetros de consulta
        String[] pairs = queryString.split("&");
        Map<String, List<String>> paramsMap = new TreeMap<>();
        
        for (String pair : pairs) {
            int idx = pair.indexOf('=');
            String key = idx > 0 ? urlEncode(pair.substring(0, idx)) : urlEncode(pair);
            String value = idx > 0 && pair.length() > idx + 1 ? urlEncode(pair.substring(idx + 1)) : "";
            
            if (!paramsMap.containsKey(key)) {
                paramsMap.put(key, new ArrayList<>());
            }
            paramsMap.get(key).add(value);
        }
        
        // Construir la cadena canónica de parámetros
        StringBuilder result = new StringBuilder();
        boolean first = true;
        
        for (Map.Entry<String, List<String>> param : paramsMap.entrySet()) {
            List<String> values = param.getValue();
            Collections.sort(values);
            
            for (String value : values) {
                if (!first) {
                    result.append('&');
                }
                result.append(param.getKey()).append('=').append(value);
                first = false;
            }
        }
        
        return result.toString();
    }
    
    /**
     * Codifica una cadena para URL
     */
    private String urlEncode(String value) {
        try {
            // Solo codificar caracteres que AWS requiere codificar
            return value
                    .replace("+", "%20")
                    .replace("*", "%2A")
                    .replace("%7E", "~")
                    .replace("%2F", "/");
        } catch (Exception e) {
            return value;
        }
    }
}

/**
 * 3. CLASE DE PROPIEDADES DE AWS
 */
package com.empresa.connector.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "aws")
public class AwsProperties {
    
    private String defaultInstance;
    private String datetimeFormat;
    private String dateFormat;
    private String algorithm;
    private String contentType;
    private Integer connectionTimeout;
    private Integer responseTimeout;
    
    private Reconnection reconnection;
    
    private Iam iam;
    private EventBridge eventbridge;
    
    @Data
    public static class Reconnection {
        private Integer frequency;
        private Integer attempts;
    }
    
    @Data
    public static class Iam {
        private String uri;
        private String method;
        private String service;
        private String queryParams;
        private String operation;
        private String hash;
        private String sessionName;
        
        private AwsInstanceProperties aws1;
        private AwsInstanceProperties aws2;
    }
    
    @Data
    public static class EventBridge {
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
        private Result result;
    }
    
    @Data
    public static class Result {
        private String correct;
        private String incorrect;
    }
    
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
    
    @Data
    public static class EventBridgeInstanceProperties {
        private String host;
        private String eventBusName;
        private String region;
    }
}

/**
 * 4. IMPLEMENTACIÓN MEJORADA DEL EVENTBRIDGE SERVICE
 * Utilizando el servicio de autenticación AWS
 */
package com.empresa.connector.service;

import com.empresa.connector.config.properties.AwsProperties;
import com.empresa.connector.model.AwsCredentials;
import com.empresa.connector.model.EventBridgeResponse;
import com.empresa.connector.model.eventbridge.EventBridgeMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventBridgeService {

    private final AwsProperties awsProperties;
    private final AwsIamService awsIamService;
    private final AwsAuthService awsAuthService;
    private final ObjectMapper objectMapper;
    
    /**
     * Envía un mensaje a EventBridge usando AWS1
     */
    public String sendToEventBridgeWithAws1Config(String message) {
        try {
            // Obtener credenciales
            AwsCredentials credentials = awsIamService.getAwsCredentialsForAws1();
            
            // Configuración de EventBridge para AWS1
            AwsProperties.EventBridgeInstanceProperties aws1Props = awsProperties.getEventbridge().getAws1();
            
            // Convertir el mensaje a EventBridgeMessage si es necesario
            EventBridgeMessage eventBridgeMessage = parseMessage(message);
            
            // Construir la URL
            String url = String.format("https://%s", aws1Props.getHost());
            
            // Enviar la petición autenticada
            ResponseEntity<EventBridgeResponse> response = awsAuthService.sendAuthenticatedRequest(
                url,
                HttpMethod.POST,
                eventBridgeMessage,
                EventBridgeResponse.class,
                credentials,
                aws1Props.getRegion(),
                awsProperties.getEventbridge().getService(),
                awsProperties.getEventbridge().getAmzTarget()
            );
            
            // Verificar respuesta
            if (response.getStatusCode().is2xxSuccessful() && 
                response.getBody() != null && 
                response.getBody().getFailedEntryCount() == 0) {
                log.info("Eventos enviados correctamente a AWS1 EventBridge");
                return awsProperties.getEventbridge().getResult().getCorrect();
            } else {
                log.error("Error al enviar eventos a AWS1 EventBridge: {}", response.getBody());
                return awsProperties.getEventbridge().getResult().getIncorrect();
            }
            
        } catch (Exception e) {
            log.error("Error al enviar a AWS1 EventBridge: {}", e.getMessage(), e);
            return awsProperties.getEventbridge().getResult().getIncorrect();
        }
    }
    
    /**
     * Envía un mensaje a EventBridge usando AWS2
     */
    public String sendToEventBridgeWithAws2Config(String message) {
        try {
            // Obtener credenciales
            AwsCredentials credentials = awsIamService.getAwsCredentialsForAws2();
            
            // Configuración de EventBridge para AWS2
            AwsProperties.EventBridgeInstanceProperties aws2Props = awsProperties.getEventbridge().getAws2();
            
            // Convertir el mensaje a EventBridgeMessage si es necesario
            EventBridgeMessage eventBridgeMessage = parseMessage(message);
            
            // Construir la URL
            String url = String.format("https://%s", aws2Props.getHost());
            
            // Enviar la petición autenticada
            ResponseEntity<EventBridgeResponse> response = awsAuthService.sendAuthenticatedRequest(
                url,
                HttpMethod.POST,
                eventBridgeMessage,
                EventBridgeResponse.class,
                credentials,
                aws2Props.getRegion(),
                awsProperties.getEventbridge().getService(),
                awsProperties.getEventbridge().getAmzTarget()
            );
            
            // Verificar respuesta
            if (response.getStatusCode().is2xxSuccessful() && 
                response.getBody() != null && 
                response.getBody().getFailedEntryCount() == 0) {
                log.info("Eventos enviados correctamente a AWS2 EventBridge");
                return awsProperties.getEventbridge().getResult().getCorrect();
            } else {
                log.error("Error al enviar eventos a AWS2 EventBridge: {}", response.getBody());
                return awsProperties.getEventbridge().getResult().getIncorrect();
            }
            
        } catch (Exception e) {
            log.error("Error al enviar a AWS2 EventBridge: {}", e.getMessage(), e);
            return awsProperties.getEventbridge().getResult().getIncorrect();
        }
    }
    
    /**
     * Parsea el mensaje a EventBridgeMessage
     */
    private EventBridgeMessage parseMessage(String message) {
        try {
            if (message instanceof String) {
                return objectMapper.readValue(message, EventBridgeMessage.class);
            } else {
                return (EventBridgeMessage) message;
            }
        } catch (Exception e) {
            log.warn("Error parseando mensaje a EventBridgeMessage: {}", e.getMessage());
            
            // Intentar crear un mensaje EventBridge por defecto
            return EventBridgeMessage.builder()
                .detailType("Transfer_KO")
                .source("openbank.payments")
                .detail(message)
                .build();
        }
    }
}

/**
 * 5. MODELO DE RESPUESTA DE EVENTBRIDGE
 */
package com.empresa.connector.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventBridgeResponse {
    
    @JsonProperty("Entries")
    private List<EventEntry> entries;
    
    @JsonProperty("FailedEntryCount")
    private Integer failedEntryCount;
    
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class EventEntry {
        
        @JsonProperty("EventId")
        private String eventId;
        
        @JsonProperty("ErrorCode")
        private String errorCode;
        
        @JsonProperty("ErrorMessage")
        private String errorMessage;
    }
}

/**
 * 6. MODELO EXTENDIDO DE EVENTBRIDGE MESSAGE
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
public class EventBridgeMessage {
    
    @JsonProperty("Source")
    private String source;
    
    @JsonProperty("DetailType")
    private String detailType;
    
    @JsonProperty("Detail")
    private Object detail;
    
    @JsonProperty("EventBusName")
    private String eventBusName;
}

/**
 * 7. UTILIDAD PARA CARGAR CERTIFICADOS
 */
package com.empresa.connector.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.stream.Collectors;

@Slf4j
@Component
public class CertificateLoader {

    /**
     * Carga un certificado X509 desde un archivo
     */
    public X509Certificate loadCertificate(String certificatePath) {
        try {
            // Intentar cargar desde el sistema de archivos
            try {
                InputStream is = Files.newInputStream(Paths.get(certificatePath));
                CertificateFactory cf = CertificateFactory.getInstance("X.509");
                return (X509Certificate) cf.generateCertificate(is);
            } catch (Exception fsException) {
                // Si falla, intentar cargar desde el classpath
                InputStream is = new ClassPathResource(certificatePath).getInputStream();
                CertificateFactory cf = CertificateFactory.getInstance("X.509");
                return (X509Certificate) cf.generateCertificate(is);
            }
        } catch (Exception e) {
            log.error("Error cargando certificado: {}", e.getMessage(), e);
            throw new RuntimeException("Error cargando certificado", e);
        }
    }
    
    /**
     * Carga una clave privada desde un archivo
     */
    public PrivateKey loadPrivateKey(String keyPath) {
        try {
            // Leer el contenido del archivo
            String privateKeyPEM;
            
            // Intentar cargar desde el sistema de archivos
            try {
                privateKeyPEM = Files.readString(Paths.get(keyPath));
            } catch (Exception fsException) {
                // Si falla, intentar cargar desde el classpath
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(new ClassPathResource(keyPath).getInputStream()))) {
                    privateKeyPEM = reader.lines().collect(Collectors.joining("\n"));
                }
            }
            
            // Eliminar cabeceras y saltos de línea
            privateKeyPEM = privateKeyPEM
                    .replace("-----BEGIN PRIVATE KEY-----", "")
                    .replace("-----END PRIVATE KEY-----", "")
                    .replaceAll("\\s", "");
            
            // Decodificar la clave en base64
            byte[] encoded = Base64.getDecoder().decode(privateKeyPEM);
            
            // Crear la clave privada
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
            return keyFactory.generatePrivate(keySpec);
        } catch (Exception e) {
            log.error("Error cargando clave privada: {}", e.getMessage(), e);
            throw new RuntimeException("Error cargando clave privada", e);
        }
    }
}

/**
 * 8. CONFIGURACIÓN DEL RESTTEMPLATE
 */
package com.empresa.connector.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

@Configuration
public class RestTemplateConfig {

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate(clientHttpRequestFactory());
    }
    
    private ClientHttpRequestFactory clientHttpRequestFactory() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(30000); // 30 segundos
        factory.setReadTimeout(30000);    // 30 segundos
        return factory;
    }
}