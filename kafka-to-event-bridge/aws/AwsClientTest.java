package com.santander.sov.epppaym.sovepppaym01pymt0028v1gms.infra.client;

import com.santander.sov.epppaym.sovepppaym01pymt0028v1gms.util.AwsIamAuthGenerate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for AwsClient class
 */
@ExtendWith(MockitoExtension.class)
public class AwsClientTest {

    @Mock
    private RedisClient redisClient;
    
    @Mock
    private AwsIamAuthGenerate awsIamAuthGenerate;
    
    @Mock
    private RestTemplate restTemplate;
    
    @InjectMocks
    private AwsClient awsClient;
    
    // Test data
    private final String base64Certificate = "dGVzdENlcnRpZmljYXRl"; // "testCertificate" in Base64
    private final String base64Key = "dGVzdEtleQ=="; // "testKey" in Base64
    private final String sessionName = "test-session";
    
    // AWS1 config
    private final String iamHostAws1 = "rolesanywhere.us-east-1.amazonaws.com";
    private final String trustAnchorArnAws1 = "arn:aws:rolesanywhere:us-east-1:123456789012:trust-anchor/trustAnchor1";
    private final String profileArnAws1 = "arn:aws:rolesanywhere:us-east-1:123456789012:profile/profile1";
    private final String roleArnAws1 = "arn:aws:iam::123456789012:role/role1";
    private final String regionAws1 = "us-east-1";
    private final String eventBridgeHostAws1 = "events.us-east-1.amazonaws.com";
    
    // AWS2 config
    private final String iamHostAws2 = "rolesanywhere.us-east-2.amazonaws.com";
    private final String trustAnchorArnAws2 = "arn:aws:rolesanywhere:us-east-2:123456789012:trust-anchor/trustAnchor2";
    private final String profileArnAws2 = "arn:aws:rolesanywhere:us-east-2:123456789012:profile/profile2";
    private final String roleArnAws2 = "arn:aws:iam::123456789012:role/role2";
    private final String regionAws2 = "us-east-2";
    private final String eventBridgeHostAws2 = "events.us-east-2.amazonaws.com";
    
    @BeforeEach
    void setUp() {
        // Set field values using ReflectionTestUtils
        ReflectionTestUtils.setField(awsClient, "base64Certificate", base64Certificate);
        ReflectionTestUtils.setField(awsClient, "base64Key", base64Key);
        ReflectionTestUtils.setField(awsClient, "sessionName", sessionName);
        
        // AWS1 config
        ReflectionTestUtils.setField(awsClient, "iamHostAws1", iamHostAws1);
        ReflectionTestUtils.setField(awsClient, "trustAnchorArnAws1", trustAnchorArnAws1);
        ReflectionTestUtils.setField(awsClient, "profileArnAws1", profileArnAws1);
        ReflectionTestUtils.setField(awsClient, "roleArnAws1", roleArnAws1);
        ReflectionTestUtils.setField(awsClient, "regionAws1", regionAws1);
        ReflectionTestUtils.setField(awsClient, "eventBridgeHostAws1", eventBridgeHostAws1);
        
        // AWS2 config
        ReflectionTestUtils.setField(awsClient, "iamHostAws2", iamHostAws2);
        ReflectionTestUtils.setField(awsClient, "trustAnchorArnAws2", trustAnchorArnAws2);
        ReflectionTestUtils.setField(awsClient, "profileArnAws2", profileArnAws2);
        ReflectionTestUtils.setField(awsClient, "roleArnAws2", roleArnAws2);
        ReflectionTestUtils.setField(awsClient, "regionAws2", regionAws2);
        ReflectionTestUtils.setField(awsClient, "eventBridgeHostAws2", eventBridgeHostAws2);
        
        // Setup common mocks that all tests might need
        when(awsIamAuthGenerate.decodeBase64(any())).thenReturn("decoded".getBytes());
        when(awsIamAuthGenerate.createIamRequestBody(any(), any(), any(), any())).thenReturn(new HashMap<>());
        when(awsIamAuthGenerate.createEventBridgeHeaders(any(), any(), any(), any())).thenReturn(new HttpHeaders());
    }
    
    @Test
    void sendEventToAws1_withValidCachedCredentials_returnsTrue() {
        // Arrange
        Object event = new HashMap<String, Object>();
        
        // Create valid cached credentials
        Map<String, Object> validCredentials = new HashMap<>();
        validCredentials.put("accessKey", "cachedAccessKey");
        validCredentials.put("secretKey", "cachedSecretKey");
        validCredentials.put("sessionToken", "cachedSessionToken");
        validCredentials.put("expiration", String.valueOf(Instant.now().plusSeconds(3600).toEpochMilli()));
        
        // Mock RedisClient to return valid credentials
        when(redisClient.getAwsCredentials(any(), eq(true))).thenReturn(validCredentials);
        
        // Mock RestTemplate postForEntity
        when(restTemplate.postForEntity(any(), any(), eq(String.class))).thenReturn(mock(ResponseEntity.class));
        
        // Act
        boolean result = awsClient.sendEventToAws1(event);
        
        // Assert
        assertTrue(result);
        // We only verify the essential interactions
        verify(redisClient).getAwsCredentials(any(), eq(true));
        verify(restTemplate).postForEntity(contains(eventBridgeHostAws1), any(), eq(String.class));
    }
    
    @Test
    void sendEventToAws1_withExpiredCredentials_generatesNewCredentials() {
        // Arrange
        Object event = new HashMap<String, Object>();
        
        // Create expired credentials
        Map<String, Object> expiredCredentials = new HashMap<>();
        expiredCredentials.put("accessKey", "expiredAccessKey");
        expiredCredentials.put("secretKey", "expiredSecretKey");
        expiredCredentials.put("expiration", String.valueOf(Instant.now().minusSeconds(60).toEpochMilli())); // Expired
        
        // Mock RedisClient to return expired credentials
        when(redisClient.getAwsCredentials(any(), eq(true))).thenReturn(expiredCredentials);
        
        // Mock new credentials generation
        Map<String, Object> newCredentials = new HashMap<>();
        newCredentials.put("accessKey", "newAccessKey");
        newCredentials.put("secretKey", "newSecretKey");
        newCredentials.put("sessionToken", "newSessionToken");
        newCredentials.put("expiration", String.valueOf(Instant.now().plusSeconds(3600).toEpochMilli()));
        
        when(awsIamAuthGenerate.getAwsCredentialsFromIam(any(), any(), any())).thenReturn(newCredentials);
        
        // Mock RestTemplate postForEntity
        when(restTemplate.postForEntity(any(), any(), eq(String.class))).thenReturn(mock(ResponseEntity.class));
        
        // Act
        boolean result = awsClient.sendEventToAws1(event);
        
        // Assert
        assertTrue(result);
        // Only verify key interactions
        verify(redisClient).getAwsCredentials(any(), eq(true));
        verify(awsIamAuthGenerate).getAwsCredentialsFromIam(any(), any(), any());
        verify(redisClient).storeAwsCredentials(any(), any(), eq(true));
        verify(restTemplate).postForEntity(contains(eventBridgeHostAws1), any(), eq(String.class));
    }
    
    @Test
    void sendEventToAws1_withRestTemplateException_returnsFalse() {
        // Arrange
        Object event = new HashMap<String, Object>();
        
        // Create valid cached credentials
        Map<String, Object> validCredentials = new HashMap<>();
        validCredentials.put("accessKey", "cachedAccessKey");
        validCredentials.put("secretKey", "cachedSecretKey");
        validCredentials.put("expiration", String.valueOf(Instant.now().plusSeconds(3600).toEpochMilli()));
        
        // Mock RedisClient to return valid credentials
        when(redisClient.getAwsCredentials(any(), eq(true))).thenReturn(validCredentials);
        
        // Mock RestTemplate to throw exception
        when(restTemplate.postForEntity(any(), any(), eq(String.class)))
            .thenThrow(new RuntimeException("Connection error"));
        
        // Act
        boolean result = awsClient.sendEventToAws1(event);
        
        // Assert
        assertFalse(result);
        verify(redisClient).getAwsCredentials(any(), eq(true));
        verify(restTemplate).postForEntity(contains(eventBridgeHostAws1), any(), eq(String.class));
    }
    
    @Test
    void sendEventToAws2_withValidCachedCredentials_returnsTrue() {
        // Arrange
        Object event = new HashMap<String, Object>();
        
        // Create valid cached credentials
        Map<String, Object> validCredentials = new HashMap<>();
        validCredentials.put("accessKey", "cachedAccessKey");
        validCredentials.put("secretKey", "cachedSecretKey");
        validCredentials.put("expiration", String.valueOf(Instant.now().plusSeconds(3600).toEpochMilli()));
        
        // Mock RedisClient to return valid credentials
        when(redisClient.getAwsCredentials(any(), eq(false))).thenReturn(validCredentials);
        
        // Mock RestTemplate postForEntity
        when(restTemplate.postForEntity(any(), any(), eq(String.class))).thenReturn(mock(ResponseEntity.class));
        
        // Act
        boolean result = awsClient.sendEventToAws2(event);
        
        // Assert
        assertTrue(result);
        verify(redisClient).getAwsCredentials(any(), eq(false));
        verify(restTemplate).postForEntity(contains(eventBridgeHostAws2), any(), eq(String.class));
    }
    
    @Test
    void sendEventToAws2_withNoCredentialsInCache_generatesNewCredentials() {
        // Arrange
        Object event = new HashMap<String, Object>();
        
        // Mock RedisClient to return null (no credentials in cache)
        when(redisClient.getAwsCredentials(any(), eq(false))).thenReturn(null);
        
        // Mock new credentials generation
        Map<String, Object> newCredentials = new HashMap<>();
        newCredentials.put("accessKey", "newAccessKey");
        newCredentials.put("secretKey", "newSecretKey");
        newCredentials.put("sessionToken", "newSessionToken");
        newCredentials.put("expiration", String.valueOf(Instant.now().plusSeconds(3600).toEpochMilli()));
        
        when(awsIamAuthGenerate.getAwsCredentialsFromIam(any(), any(), any())).thenReturn(newCredentials);
        
        // Mock RestTemplate postForEntity
        when(restTemplate.postForEntity(any(), any(), eq(String.class))).thenReturn(mock(ResponseEntity.class));
        
        // Act
        boolean result = awsClient.sendEventToAws2(event);
        
        // Assert
        assertTrue(result);
        verify(redisClient).getAwsCredentials(any(), eq(false));
        verify(awsIamAuthGenerate).getAwsCredentialsFromIam(any(), any(), any());
        verify(redisClient).storeAwsCredentials(any(), any(), eq(false));
        verify(restTemplate).postForEntity(contains(eventBridgeHostAws2), any(), eq(String.class));
    }
    
    @Test
    void sendEventToAws2_withIamException_usesMockCredentials() {
        // Arrange
        Object event = new HashMap<String, Object>();
        
        // Mock RedisClient to return null (no credentials in cache)
        when(redisClient.getAwsCredentials(any(), eq(false))).thenReturn(null);
        
        // Mock IAM to throw exception
        when(awsIamAuthGenerate.getAwsCredentialsFromIam(any(), any(), any()))
            .thenThrow(new RuntimeException("IAM Error"));
        
        // Mock RestTemplate postForEntity
        when(restTemplate.postForEntity(any(), any(), eq(String.class))).thenReturn(mock(ResponseEntity.class));
        
        // Act
        boolean result = awsClient.sendEventToAws2(event);
        
        // Assert
        assertTrue(result);
        verify(redisClient).getAwsCredentials(any(), eq(false));
        verify(awsIamAuthGenerate).getAwsCredentialsFromIam(any(), any(), any());
        verify(redisClient).storeAwsCredentials(any(), any(), eq(false));
        verify(restTemplate).postForEntity(contains(eventBridgeHostAws2), any(), eq(String.class));
    }
}