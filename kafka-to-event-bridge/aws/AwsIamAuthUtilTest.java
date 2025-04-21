package com.santander.sov.epppaym.sovepppaym01pymt0028v1gms.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
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
 * Unit tests for AwsIamAuthUtil class
 */
@ExtendWith(MockitoExtension.class)
public class AwsIamAuthUtilTest {

    @InjectMocks
    private AwsIamAuthUtil awsIamAuthUtil;

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
        byte[] result = awsIamAuthUtil.decodeBase64(validBase64Certificate);
        
        // Assert
        assertArrayEquals(decodedCertificateBytes, result);
    }

    @Test
    void decodeBase64_withWhitespace_returnsDecodedBytes() {
        // Arrange
        String base64WithWhitespace = validBase64Certificate.substring(0, 5) + " \n\t" + 
                                      validBase64Certificate.substring(5);
        
        // Act
        byte[] result = awsIamAuthUtil.decodeBase64(base64WithWhitespace);
        
        // Assert
        assertArrayEquals(decodedCertificateBytes, result);
    }

    @Test
    void decodeBase64_nullInput_throwsIllegalArgumentException() {
        // Act & Assert
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class, 
                () -> awsIamAuthUtil.decodeBase64(null)
        );
        
        assertTrue(exception.getMessage().contains("cannot be null or empty"));
    }

    @Test
    void decodeBase64_emptyInput_throwsIllegalArgumentException() {
        // Act & Assert
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class, 
                () -> awsIamAuthUtil.decodeBase64("")
        );
        
        assertTrue(exception.getMessage().contains("cannot be null or empty"));
    }

    @Test
    void decodeBase64_invalidInput_throwsIllegalArgumentException() {
        // Act & Assert
        assertThrows(
                IllegalArgumentException.class, 
                () -> awsIamAuthUtil.decodeBase64("!@#$%^&*")
        );
    }

    @Test
    void convertToPem_validInput_returnsPemFormat() {
        // Act
        String result = awsIamAuthUtil.convertToPem(decodedCertificateBytes);
        
        // Assert
        assertTrue(result.startsWith("-----BEGIN CERTIFICATE-----"));
        assertTrue(result.endsWith("-----END CERTIFICATE-----"));
        assertTrue(result.contains(Base64.getEncoder().encodeToString(decodedCertificateBytes)));
    }

    @Test
    void convertKeyToPem_validInput_returnsPemFormat() {
        // Act
        String result = awsIamAuthUtil.convertKeyToPem(decodedKeyBytes);
        
        // Assert
        assertTrue(result.startsWith("-----BEGIN PRIVATE KEY-----"));
        assertTrue(result.endsWith("-----END PRIVATE KEY-----"));
        assertTrue(result.contains(Base64.getEncoder().encodeToString(decodedKeyBytes)));
    }

    @Test
    void createIamAuthHeaders_returnsValidHeaders() {
        // Act
        HttpHeaders headers = awsIamAuthUtil.createIamAuthHeaders();
        
        // Assert
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
        HttpHeaders headers = awsIamAuthUtil.createEventBridgeHeaders(
                accessKey, secretKey, sessionToken, region);
        
        // Assert
        assertEquals("application/x-amz-json-1.1", headers.getFirst("Content-Type"));
        assertEquals("AWSEvents.PutEvents", headers.getFirst("X-Amz-Target"));
        assertNotNull(headers.getFirst("X-Amz-Date"));
        assertEquals(sessionToken, headers.getFirst("X-Amz-Security-Token"));
        assertNotNull(headers.getFirst("Authorization"));
        assertTrue(headers.getFirst("Authorization").contains("AWS4-HMAC-SHA256"));
        assertTrue(headers.getFirst("Authorization").contains(accessKey));
        assertTrue(headers.getFirst("Authorization").contains(region));
    }

    @Test
    void createEventBridgeHeaders_withoutSessionToken_returnsValidHeaders() {
        // Arrange
        String accessKey = "testAccessKey";
        String secretKey = "testSecretKey";
        String region = "us-east-1";
        
        // Act
        HttpHeaders headers = awsIamAuthUtil.createEventBridgeHeaders(
                accessKey, secretKey, null, region);
        
        // Assert
        assertEquals("application/x-amz-json-1.1", headers.getFirst("Content-Type"));
        assertEquals("AWSEvents.PutEvents", headers.getFirst("X-Amz-Target"));
        assertNotNull(headers.getFirst("X-Amz-Date"));
        assertNull(headers.getFirst("X-Amz-Security-Token"));
        assertNotNull(headers.getFirst("Authorization"));
        assertTrue(headers.getFirst("Authorization").contains("AWS4-HMAC-SHA256"));
        assertTrue(headers.getFirst("Authorization").contains(accessKey));
        assertTrue(headers.getFirst("Authorization").contains(region));
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
        
        when(restTemplate.postForEntity(
                anyString(), 
                any(HttpEntity.class), 
                eq(Map.class)
        )).thenReturn(mockResponse);
        
        // Act
        Map<String, Object> result = awsIamAuthUtil.getAwsCredentialsFromIam(
                iamHost, requestBody, restTemplate);
        
        // Assert
        assertNotNull(result);
        assertEquals("testAccessKey", result.get("accessKey"));
        assertEquals("testSecretKey", result.get("secretKey"));
        assertEquals("testSessionToken", result.get("sessionToken"));
        assertEquals("2023-12-31T23:59:59Z", result.get("expiration"));
        
        // Verify
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
        
        ResponseEntity<Map> mockResponse = new ResponseEntity<>(HttpStatus.UNAUTHORIZED);
        
        when(restTemplate.postForEntity(
                anyString(), 
                any(HttpEntity.class), 
                eq(Map.class)
        )).thenReturn(mockResponse);
        
        // Act & Assert
        RuntimeException exception = assertThrows(
                RuntimeException.class, 
                () -> awsIamAuthUtil.getAwsCredentialsFromIam(iamHost, requestBody, restTemplate)
        );
        
        assertTrue(exception.getMessage().contains("Error getting credentials from IAM"));
    }

    @Test
    void getAwsCredentialsFromIam_restTemplateException_throwsException() {
        // Arrange
        String iamHost = "rolesanywhere.us-east-1.amazonaws.com";
        Map<String, Object> requestBody = new HashMap<>();
        
        when(restTemplate.postForEntity(
                anyString(), 
                any(HttpEntity.class), 
                eq(Map.class)
        )).thenThrow(new RuntimeException("Connection error"));
        
        // Act & Assert
        RuntimeException exception = assertThrows(
                RuntimeException.class, 
                () -> awsIamAuthUtil.getAwsCredentialsFromIam(iamHost, requestBody, restTemplate)
        );
        
        assertTrue(exception.getMessage().contains("Error getting credentials from IAM"));
    }

    @Test
    void createIamRequestBody_withAllParams_returnsValidRequestBody() {
        // Arrange
        String trustAnchorArn = "test-trust-anchor-arn";
        String profileArn = "test-profile-arn";
        String roleArn = "test-role-arn";
        String sessionName = "test-session-name";
        
        // Act
        Map<String, Object> result = awsIamAuthUtil.createIamRequestBody(
                trustAnchorArn, profileArn, roleArn, sessionName);
        
        // Assert
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
        Map<String, Object> result = awsIamAuthUtil.createIamRequestBody(
                trustAnchorArn, profileArn, roleArn, null);
        
        // Assert
        assertNotNull(result);
        assertEquals(trustAnchorArn, result.get("trustAnchorArn"));
        assertEquals(profileArn, result.get("profileArn"));
        assertEquals(roleArn, result.get("roleArn"));
        assertFalse(result.containsKey("sessionName"));
        assertEquals(3600, result.get("durationSeconds"));
    }
}