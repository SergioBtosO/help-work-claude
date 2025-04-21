package com.santander.sov.epppaym.sovepppaym01pymt0028v1gms.infra.client;

import com.santander.sov.epppaym.sovepppaym01pymt0028v1gms.util.AwsIamAuthGenerate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Client for AWS services including IAM and EventBridge
 */
@Component
public class AwsClient {
    
    private static final Logger logger = LoggerFactory.getLogger(AwsClient.class);
    
    private final RedisClient redisClient;
    private final AwsIamAuthGenerate awsIamAuthGenerate;
    private final RestTemplate restTemplate;
    
    @Value("${aws.iam.certificate}")
    private String base64Certificate;
    
    @Value("${aws.iam.key}")
    private String base64Key;
    
    @Value("${aws.iam.session-name}")
    private String sessionName;
    
    // AWS1 configuración
    @Value("${aws.aws1.iam.host}")
    private String iamHostAws1;
    
    @Value("${aws.aws1.iam.trust-anchor-arn}")
    private String trustAnchorArnAws1;
    
    @Value("${aws.aws1.iam.profile-arn}")
    private String profileArnAws1;
    
    @Value("${aws.aws1.iam.role-arn}")
    private String roleArnAws1;
    
    @Value("${aws.aws1.iam.region}")
    private String regionAws1;
    
    @Value("${aws.aws1.iam.event-bridge.host}")
    private String eventBridgeHostAws1;
    
    // AWS2 configuración
    @Value("${aws.aws2.iam.host}")
    private String iamHostAws2;
    
    @Value("${aws.aws2.iam.trust-anchor-arn}")
    private String trustAnchorArnAws2;
    
    @Value("${aws.aws2.iam.profile-arn}")
    private String profileArnAws2;
    
    @Value("${aws.aws2.iam.role-arn}")
    private String roleArnAws2;
    
    @Value("${aws.aws2.iam.region}")
    private String regionAws2;
    
    @Value("${aws.aws2.iam.event-bridge.host}")
    private String eventBridgeHostAws2;
    
    private static final String AWS_CREDENTIALS_KEY = "aws-credentials";
    
    /**
     * Constructor with explicit dependency injection
     */
    @Autowired
    public AwsClient(RedisClient redisClient, AwsIamAuthGenerate awsIamAuthGenerate, RestTemplate restTemplate) {
        this.redisClient = redisClient;
        this.awsIamAuthGenerate = awsIamAuthGenerate;
        this.restTemplate = restTemplate;
    }
    
    /**
     * Sends an event to AWS EventBridge using AWS1
     * 
     * @param event Object representing the event to send
     * @return true if the sending was successful, false otherwise
     */
    public boolean sendEventToAws1(Object event) {
        try {
            logger.info("Initiating event sending to AWS1 EventBridge");
            
            // Get valid credentials for AWS1
            Map<String, Object> credentials = getValidAwsCredentials(true);
            
            // Send the event using the credentials
            return sendEventToEventBridge(
                    event, 
                    credentials, 
                    eventBridgeHostAws1, 
                    regionAws1);
            
        } catch (Exception e) {
            logger.error("Error sending event to AWS1: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * Sends an event to AWS EventBridge using AWS2
     * 
     * @param event Object representing the event to send
     * @return true if the sending was successful, false otherwise
     */
    public boolean sendEventToAws2(Object event) {
        try {
            logger.info("Initiating event sending to AWS2 EventBridge");
            
            // Get valid credentials for AWS2
            Map<String, Object> credentials = getValidAwsCredentials(false);
            
            // Send the event using the credentials
            return sendEventToEventBridge(
                    event, 
                    credentials, 
                    eventBridgeHostAws2, 
                    regionAws2);
            
        } catch (Exception e) {
            logger.error("Error sending event to AWS2: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * Gets valid AWS credentials, checking first in Redis
     * and generating new ones through IAM if necessary
     * 
     * @param isAws1 true for AWS1, false for AWS2
     * @return Map with valid credentials
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> getValidAwsCredentials(boolean isAws1) {
        logger.debug("Checking AWS credentials in Redis for {}", isAws1 ? "AWS1" : "AWS2");
        
        // 1. Check if credentials exist in Redis
        Map<String, Object> cachedCredentials = redisClient.getAwsCredentials(AWS_CREDENTIALS_KEY, isAws1);
        
        // 2. Check if credentials are valid (exist and not expired)
        if (areCredentialsValid(cachedCredentials)) {
            logger.debug("Valid credentials found in Redis");
            return cachedCredentials;
        }
        
        logger.info("No valid credentials found in Redis, generating new ones for {}", 
                isAws1 ? "AWS1" : "AWS2");
        
        // 3. Generate new credentials through IAM
        Map<String, Object> newCredentials = generateAwsCredentialsFromIam(isAws1);
        
        // 4. Store the new credentials in Redis
        redisClient.storeAwsCredentials(AWS_CREDENTIALS_KEY, newCredentials, isAws1);
        
        return newCredentials;
    }
    
    /**
     * Checks if credentials are valid (exist and not expired)
     * 
     * @param credentials Credentials to check
     * @return true if valid, false otherwise
     */
    private boolean areCredentialsValid(Map<String, Object> credentials) {
        if (credentials == null) {
            return false;
        }
        
        // Check if they have the required fields
        if (!credentials.containsKey("accessKey") || 
            !credentials.containsKey("secretKey") || 
            !credentials.containsKey("expiration")) {
            return false;
        }
        
        // Check if they have expired
        try {
            long expirationTime = Long.parseLong(credentials.get("expiration").toString());
            long currentTime = Instant.now().toEpochMilli();
            
            // Consider valid if more than 5 minutes until expiration
            return (expirationTime - currentTime) > (5 * 60 * 1000);
        } catch (NumberFormatException e) {
            logger.warn("Invalid expiration time format in stored credentials");
            return false;
        }
    }
    
    /**
     * Generates new AWS credentials by calling IAM with certificate and key
     * 
     * @param isAws1 true for AWS1, false for AWS2
     * @return Map with new credentials
     */
    private Map<String, Object> generateAwsCredentialsFromIam(boolean isAws1) {
        try {
            logger.info("Generating new AWS credentials through IAM");
            
            // Configure specific parameters based on environment (AWS1 or AWS2)
            String iamHost = isAws1 ? iamHostAws1 : iamHostAws2;
            String trustAnchorArn = isAws1 ? trustAnchorArnAws1 : trustAnchorArnAws2;
            String profileArn = isAws1 ? profileArnAws1 : profileArnAws2;
            String roleArn = isAws1 ? roleArnAws1 : roleArnAws2;
            
            // 1. Prepare request body for IAM
            Map<String, Object> requestBody = awsIamAuthGenerate.createIamRequestBody(
                    trustAnchorArn,
                    profileArn,
                    roleArn,
                    sessionName);
            
            // 2. Decode certificate and key
            byte[] certificateBytes = awsIamAuthGenerate.decodeBase64(base64Certificate);
            byte[] keyBytes = awsIamAuthGenerate.decodeBase64(base64Key);
            
            // In a real implementation, these would be used to configure SSL
            
            // 3. Make the call to IAM using shared RestTemplate
            Map<String, Object> credentials = awsIamAuthGenerate.getAwsCredentialsFromIam(
                    iamHost,
                    requestBody,
                    this.restTemplate);
            
            logger.info("AWS credentials generated successfully");
            return credentials;
            
        } catch (Exception e) {
            logger.error("Error generating AWS credentials: {}", e.getMessage(), e);
            
            // For development only - REMOVE IN PRODUCTION
            return createMockCredentials(isAws1);
        }
    }
    
    /**
     * Creates mock credentials for development/testing
     * FOR DEVELOPMENT ONLY - REMOVE IN PRODUCTION
     */
    private Map<String, Object> createMockCredentials(boolean isAws1) {
        logger.warn("Creating mock AWS credentials (DEVELOPMENT ONLY)");
        
        Map<String, Object> credentials = new HashMap<>();
        credentials.put("accessKey", isAws1 ? "mock-access-key-aws1" : "mock-access-key-aws2");
        credentials.put("secretKey", isAws1 ? "mock-secret-key-aws1" : "mock-secret-key-aws2");
        credentials.put("sessionToken", isAws1 ? "mock-session-token-aws1" : "mock-session-token-aws2");
        
        // Expiration in 1 hour from now
        long expirationTime = Instant.now().plusSeconds(3600).toEpochMilli();
        credentials.put("expiration", String.valueOf(expirationTime));
        
        return credentials;
    }
    
    /**
     * Sends an event to AWS EventBridge
     * 
     * @param event The event to send
     * @param credentials AWS credentials
     * @param eventBridgeHost EventBridge host
     * @param region AWS region
     * @return true if sending was successful, false otherwise
     */
    private boolean sendEventToEventBridge(
            Object event,
            Map<String, Object> credentials,
            String eventBridgeHost,
            String region) {
        
        try {
            logger.debug("Preparing sending to EventBridge: {}", eventBridgeHost);
            
            // Extract credentials
            String accessKey = credentials.get("accessKey").toString();
            String secretKey = credentials.get("secretKey").toString();
            String sessionToken = credentials.containsKey("sessionToken") ? 
                    credentials.get("sessionToken").toString() : null;
            
            // Create headers for EventBridge
            HttpHeaders headers = awsIamAuthGenerate.createEventBridgeHeaders(
                    accessKey,
                    secretKey,
                    sessionToken,
                    region);
            
            // Create HTTP entity
            HttpEntity<Object> requestEntity = new HttpEntity<>(event, headers);
            
            // URL for EventBridge
            String eventBridgeUrl = "https://" + eventBridgeHost;
            
            logger.info("Sending event to EventBridge: {}", eventBridgeUrl);
            
            // Send request to EventBridge using shared RestTemplate
            restTemplate.postForEntity(eventBridgeUrl, requestEntity, String.class);
            
            logger.info("Event successfully sent to EventBridge");
            return true;
            
        } catch (Exception e) {
            logger.error("Error sending event to EventBridge: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * Provides a shared RestTemplate bean 
     * This ensures the same instance is used throughout the application
     */
    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }
}