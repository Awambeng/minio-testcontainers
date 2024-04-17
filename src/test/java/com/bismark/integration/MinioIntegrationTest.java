package com.bismark.integration;

import io.minio.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@Testcontainers
public class MinioIntegrationTest {

    private static final int port = 9000;
    private static final String accessKey = "testuser";
    private static final String secretKey = "testpassword";

    // creating a miniocontainer and configuring it with username, password and exposed port.
    @Container
    private static final GenericContainer<?> minioContainer = new GenericContainer<>(DockerImageName.parse("minio/minio"))
                .withExposedPorts(port)
                .withEnv("MINIO_ACCESS_KEY", accessKey)
                .withEnv("MINIO_SECRET_KEY", secretKey)
                .withCommand("server /data");

    // using miniocontainer object to retrieve server's endpoint for minioclient configuration
    MinioClient minioClient = MinioClient
            .builder()
            .endpoint("http://" + minioContainer.getHost() + ":" + minioContainer.getMappedPort(port))
            .credentials(accessKey, secretKey)
            .build();

    @Test
    void createBucketTest() throws Exception {
        String bucketName = "my-bucket";
        log.info("Creating a bucket...");

        // create the bucket
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());

        // verify if bucket was created
        boolean bucketCreated = minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
        assertTrue(bucketCreated);
        log.info("Bucket created: {}", bucketName);
    }

    @Test
    void uploadFileTest() throws Exception {
        String bucketName = "my-bucket";
        String objectName = "my-object";
        String content = "Hello, Minio!";

        // upload the file
        try (InputStream inputStream = new ByteArrayInputStream(content.getBytes())) {
            minioClient.putObject(PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .stream(inputStream, content.length(), -1)
                    .build());
        }

        // verify if file was created
        boolean objectExists = minioClient.statObject(
                StatObjectArgs.builder().bucket(bucketName).object(objectName).build()
        ) != null;
        assertTrue(objectExists);
        log.info("File exists: {}", objectExists);

        // verify file content
        try (InputStream objectStream = minioClient.getObject(
                io.minio.GetObjectArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .build())) {
            byte[] retrievedBytes = objectStream.readAllBytes();
            String retrievedContent = new String(retrievedBytes, StandardCharsets.UTF_8);
            assertEquals(content, retrievedContent);
            log.info("File content: {}", retrievedContent);
        }
    }
}
