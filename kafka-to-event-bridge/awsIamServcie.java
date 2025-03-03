package com.empresa.connector.service;

import com.empresa.connector.model.AwsCredentials;

/**
 * Service interface for AWS IAM operations
 */
public interface AwsIamService {
    
    /**
     * Gets AWS credentials for AWS1
     * 
     * @return AWS credentials for AWS1
     */
    AwsCredentials getAwsCredentialsForAws1();
    
    /**
     * Gets AWS credentials for AWS2
     * 
     * @return AWS credentials for AWS2
     */
    AwsCredentials getAwsCredentialsForAws2();
    
    /**
     * Checks if credentials have expired
     * 
     * @param awsKey The AWS key identifier
     * @return true if credentials have expired, false otherwise
     */
    boolean hasCredentialsExpired(String awsKey);
    
    /**
     * Refreshes expired credentials
     * 
     * @param awsKey The AWS key identifier
     * @return New AWS credentials
     */
    AwsCredentials refreshCredentials(String awsKey);
}

package com.empresa.connector.service.impl;

import com.empresa.connector.config.properties.AwsIamProperties;
import com.empresa.connector.model.AwsCredentials;
import com.empresa.connector.service.AwsIamService;
import com.empresa.connector.service.RedisService;
import com.empresa.connector.util.CertificateLoader;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class AwsIamServiceImpl implements AwsIamService {

    private final AwsIamProperties iamProperties;
    private final RedisService redisService;
    private final CertificateLoader certificateLoader;
    
    @Value("${redis.expiration-time:3500}")
    private long credentialsExpirationTime;

    @Override
    public AwsCredentials getAwsCredentialsForAws1() {
        return getAwsCredentials("aws1", iamProperties.getAws1());
    }
    
    @Override
    public AwsCredentials getAwsCredentialsForAws2() {
        return getAwsCredentials("aws2", iamProperties.getAws2());
    }
    
    @Override
    public boolean hasCredentialsExpired(String awsKey) {
        String cacheKey = String.format("aws:credentials:%s", awsKey);
        return !redisService.hasKey(cacheKey) || redisService.getExpire(cacheKey) < 60; // Consider expired if less than 60 seconds left
    }
    
    @Override
    public AwsCredentials refreshCredentials(String awsKey) {
        if ("aws1".equals(awsKey)) {
            return getAwsCredentialsForAws1();
        } else if ("aws2".equals(awsKey)) {
            return getAwsCredentialsForAws2();
        } else {
            throw new IllegalArgumentException("Invalid AWS key: " + awsKey);
        }
    }
    
    /**
     * Gets AWS credentials for the specified AWS instance
     */
    private AwsCredentials getAwsCredentials(String awsKey, AwsIamProperties.AwsInstanceProperties properties) {
        // Check Redis cache first
        String cacheKey = String.format("aws:credentials:%s", awsKey);
        String cachedCredentials = redisService.getCache(cacheKey);
        
        if (cachedCredentials != null && !cachedCredentials.isEmpty()) {
            log.info("Retrieving credentials from Redis for {}", awsKey);
            try {
                return AwsCredentials.fromJson(cachedCredentials);
            } catch (Exception e) {
                log.warn("Error deserializing credentials from Redis: {}", e.getMessage());
                // Continue with getting new credentials
            }
        }
        
        try {
            log.info("Generating new credentials for {}", awsKey);
            
            // In a real implementation, load certificates and make API call to AWS IAM RolesAnywhere
            // X509Certificate certificate = certificateLoader.loadCertificate(properties.getCertificate());
            // PrivateKey privateKey = certificateLoader.loadPrivateKey(properties.getKey());
            
            // For this example, generate dummy credentials
            AwsCredentials credentials = AwsCredentials.builder()
                    .accessKeyId("ASIA" + generateRandomString(16))
                    .secretAccessKey(generateRandomString(40))
                    .sessionToken(UUID.randomUUID().toString().replace("-", ""))
                    .region(properties.getRegion())
                    .build();
            
            // Store in Redis
            try {
                redisService.setCache(cacheKey, credentials.toJson(), credentialsExpirationTime);
            } catch (Exception e) {
                log.warn("Error caching credentials in Redis: {}", e.getMessage());
            }
            
            return credentials;
        } catch (Exception e) {
            log.error("Error obtaining AWS credentials for {}: {}", awsKey, e.getMessage(), e);
            throw new RuntimeException("Error obtaining AWS credentials", e);
        }
    }
    
    /**
     * Generates a random string for demo purposes
     */
    private String generateRandomString(int length) {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            int index = (int) (chars.length() * Math.random());
            sb.append(chars.charAt(index));
        }
        return sb.toString();
    }
}