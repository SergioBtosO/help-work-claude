package com.santander.sov.epppaym.sovepppaym01pymt0028v1gms.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for AwsIamAuthGenerate class
 */
@ExtendWith(MockitoExtension.class)
public class AwsIamAuthGenerateTest {

    @InjectMocks
    private AwsIamAuthGenerate awsIamAuthGenerate;

    @Mock
    private RestTemplate restTemplate;

    private String validBase64Certificate;
    private String validBase64Key;
    private byte[] decodedCertificateBytes;
    private byte[] decodedKeyBytes;

    @BeforeEach
    void setUp() {
        // Prepare test data
        String sampleCertificate = "Sample certificate content";
        String sampleKey = "Sample key content";
        
        decodedCertificateBytes = sampleCertificate.getBytes();
        decodedKeyBytes = sampleKey.getBytes();
        
        validBase64Certificate = Base64.getEncoder().encodeToString(decodedCertificateBytes);
        validBase64Key = Base64.getEncoder().encodeToString(decodedKeyBytes);
    }

    @Test
    void decodeBase64_validInput_returnsDecodedBytes() {
        // Act
        byte[] result = awsIamAuthGenerate.decodeBase64(validBase64Certificate);
        
        // Assert
        assertArrayEquals(decodedCertificateBytes, result);
    }

    @Test
    void decodeBase64_withWhitespace_returnsDecodedBytes() {
        // Arrange
        String base64WithWhitespace = validBase64Certificate.substring(0, 5) + " \n\t" + 
                                      validBase64Certificate.substring(5);
        
        // Act
        byte[] result = awsIamAuthGenerate.decodeBase64(base64WithWhitespace);
        
        // Assert
        assertArrayEquals(decodedCertificateBytes, result);
    }

    @Test
    void decodeBase64_nullInput_throwsIllegalArgumentException() {
        // Act & Assert
        assertThrows(
                IllegalArgumentException.class, 
                () -> awsIamAuthGenerate.decodeBase64(null)
        );
    }

    @Test
    void decodeBase64_emptyInput_throwsIllegalArgumentException() {
        // Act & Assert
        assertThrows(
                IllegalArgumentException.class, 
                () -> awsIamAuthGenerate.decodeBase64("")
        );
    }

    @Test
    void decodeBase64_invalidInput_throwsIllegalArgumentException() {
        // Act & Assert
        assertThrows(
                IllegalArgumentException.class, 
                () -> awsIamAuthGenerate.decodeBase64("!@#$%^&*")
        );
    }

    @Test
    void convertToPem_validInput_returnsPemFormat() {
        // Act
        String result = awsIamAuthGenerate.convertToPem(decodedCertificateBytes);
        
        // Assert - Check basic PEM structure
        assertTrue(result.startsWith("-----BEGIN CERTIFICATE-----"));
        assertTrue(result.endsWith("-----END CERTIFICATE-----"));
        assertTrue(result.contains(Base64.getEncoder().encodeToString(decodedCertificateBytes)));
    }

    @Test
    void convertKeyToPem_validInput_returnsPemFormat() {
        // Act
        String result = awsIamAuthGenerate.convertKeyToPem(decodedKeyBytes);
        
        // Assert - Check basic PEM structure
        assertTrue(result.startsWith("-----BEGIN PRIVATE KEY-----"));
        assertTrue(result.endsWith("-----END PRIVATE KEY-----"));
        assertTrue(result.contains(Base64.getEncoder().encodeToString(decodedKeyBytes)));
    }

    @Test
    void createIamAuthHeaders_returnsValidHeaders() {
        // Act
        HttpHeaders headers = awsIamAuthGenerate.createIamAuthHeaders();
        
        // Assert - Check required headers
        assertEquals("application/json", headers.getContentType().toString());
        assertNotNull(headers.getFirst("X-Amz-Date"));
        assertNotNull(headers.getFirst("Accept"));
    }

    @Test
    void createEventBridgeHeaders_withSessionToken_returnsValidHeaders() {
        // Arrange
        String accessKey = "testAccessKey";
        String secretKey = "testSecretKey";
        String sessionToken = "testSessionToken";
        String region = "us-east-1";
        
        // Act
        HttpHeaders headers = awsIamAuthGenerate.createEventBridgeHeaders(
                accessKey, secretKey, sessionToken, region);
        
        // Assert - Check required headers
        assertEquals("application/x-amz-json-1.1", headers.getFirst("Content-Type"));
        assertEquals("AWSEvents.PutEvents", headers.getFirst("X-Amz-Target"));
        assertNotNull(headers.getFirst("X-Amz-Date"));
        assertEquals(sessionToken, headers.getFirst("X-Amz-Security-Token"));
        assertNotNull(headers.getFirst("Authorization"));
    }

    @Test
    void createEventBridgeHeaders_withoutSessionToken_returnsValidHeaders() {
        // Arrange
        String accessKey = "testAccessKey";
        String secretKey = "testSecretKey";
        String region = "us-east-1";
        
        // Act
        HttpHeaders headers = awsIamAuthGenerate.createEventBridgeHeaders(
                accessKey, secretKey, null, region);
        
        // Assert - Check required headers
        assertEquals("application/x-amz-json-1.1", headers.getFirst("Content-Type"));
        assertEquals("AWSEvents.PutEvents", headers.getFirst("X-Amz-Target"));
        assertNotNull(headers.getFirst("X-Amz-Date"));
        assertNull(headers.getFirst("X-Amz-Security-Token"));
        assertNotNull(headers.getFirst("Authorization"));
    }

    @Test
    void getAwsCredentialsFromIam_success_returnsCredentials() {
        // Arrange
        String iamHost = "rolesanywhere.us-east-1.amazonaws.com";
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("profileArn", "test-profile-arn");
        
        // Mock response
        Map<String, Object> credentialsMap = new HashMap<>();
        credentialsMap.put("accessKeyId", "testAccessKey");
        credentialsMap.put("secretAccessKey", "testSecretKey");
        credentialsMap.put("sessionToken", "testSessionToken");
        credentialsMap.put("expiration", "2023-12-31T23:59:59Z");
        
        Map<String, Object> responseBody = new HashMap<>();
        responseBody.put("credentials", credentialsMap);
        
        ResponseEntity<Map> mockResponse = new ResponseEntity<>(responseBody, HttpStatus.OK);
        
        // Configurar el mock para cualquier URL y cualquier HttpEntity
        when(restTemplate.postForEntity(
                anyString(), 
                any(HttpEntity.class), 
                eq(Map.class)
        )).thenReturn(mockResponse);
        
        // Act
        Map<String, Object> result = awsIamAuthGenerate.getAwsCredentialsFromIam(
                iamHost, requestBody, restTemplate);
        
        // Assert - Check returned credentials
        assertNotNull(result);
        assertEquals("testAccessKey", result.get("accessKey"));
        assertEquals("testSecretKey", result.get("secretKey"));
        assertEquals("testSessionToken", result.get("sessionToken"));
        assertEquals("2023-12-31T23:59:59Z", result.get("expiration"));
        
        // Verify - Check that restTemplate was called with correct parameters
        verify(restTemplate).postForEntity(
                eq("https://" + iamHost + "/sessions"), 
                any(HttpEntity.class), 
                eq(Map.class)
        );
    }

    @Test
    void getAwsCredentialsFromIam_errorResponse_throwsException() {
        // Arrange
        String iamHost = "rolesanywhere.us-east-1.amazonaws.com";
        Map<String, Object> requestBody = new HashMap<>();
        
        // Crear una respuesta con error 401
        ResponseEntity<Map> mockResponse = new ResponseEntity<>(HttpStatus.UNAUTHORIZED);
        
        // Configurar mock para devolver error independientemente de los parámetros
        when(restTemplate.postForEntity(
                anyString(), 
                any(HttpEntity.class), 
                eq(Map.class)
        )).thenReturn(mockResponse);
        
        // Act & Assert - Just verify that exception is thrown
        assertThrows(
                RuntimeException.class, 
                () -> awsIamAuthGenerate.getAwsCredentialsFromIam(iamHost, requestBody, restTemplate)
        );
        
        // Verify - Check that restTemplate was called with correct parameters
        verify(restTemplate).postForEntity(
                eq("https://" + iamHost + "/sessions"), 
                any(HttpEntity.class), 
                eq(Map.class)
        );
    }

    @Test
    void getAwsCredentialsFromIam_restTemplateException_throwsException() {
        // Arrange
        String iamHost = "rolesanywhere.us-east-1.amazonaws.com";
        Map<String, Object> requestBody = new HashMap<>();
        
        // Configurar el mock para lanzar una excepción cuando se llame
        when(restTemplate.postForEntity(
                anyString(), 
                any(HttpEntity.class), 
                eq(Map.class)
        )).thenThrow(new RuntimeException("Connection error"));
        
        // Act & Assert - Just verify that exception is thrown
        assertThrows(
                RuntimeException.class, 
                () -> awsIamAuthGenerate.getAwsCredentialsFromIam(iamHost, requestBody, restTemplate)
        );
        
        // Verify - Check that restTemplate was called with correct parameters
        verify(restTemplate).postForEntity(
                eq("https://" + iamHost + "/sessions"), 
                any(HttpEntity.class), 
                eq(Map.class)
        );
    }

    @Test
    void createIamRequestBody_withAllParams_returnsValidRequestBody() {
        // Arrange
        String trustAnchorArn = "test-trust-anchor-arn";
        String profileArn = "test-profile-arn";
        String roleArn = "test-role-arn";
        String sessionName = "test-session-name";
        
        // Act
        Map<String, Object> result = awsIamAuthGenerate.createIamRequestBody(
                trustAnchorArn, profileArn, roleArn, sessionName);
        
        // Assert - Check returned request body
        assertNotNull(result);
        assertEquals(trustAnchorArn, result.get("trustAnchorArn"));
        assertEquals(profileArn, result.get("profileArn"));
        assertEquals(roleArn, result.get("roleArn"));
        assertEquals(sessionName, result.get("sessionName"));
        assertEquals(3600, result.get("durationSeconds"));
    }

    @Test
    void createIamRequestBody_withoutSessionName_returnsValidRequestBody() {
        // Arrange
        String trustAnchorArn = "test-trust-anchor-arn";
        String profileArn = "test-profile-arn";
        String roleArn = "test-role-arn";
        
        // Act
        Map<String, Object> result = awsIamAuthGenerate.createIamRequestBody(
                trustAnchorArn, profileArn, roleArn, null);
        
        // Assert - Check returned request body
        assertNotNull(result);
        assertEquals(trustAnchorArn, result.get("trustAnchorArn"));
        assertEquals(profileArn, result.get("profileArn"));
        assertEquals(roleArn, result.get("roleArn"));
        assertFalse(result.containsKey("sessionName"));
        assertEquals(3600, result.get("durationSeconds"));
    }
}