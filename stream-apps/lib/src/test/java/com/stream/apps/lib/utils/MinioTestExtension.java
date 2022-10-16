package com.stream.apps.lib.utils;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.stream.apps.lib.utils.fields.MinioClientField;
import java.util.Arrays;
import org.junit.jupiter.api.extension.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class MinioTestExtension extends AbstractTestExtension
    implements BeforeEachCallback, AfterEachCallback {
  private final String accessKey = "minio_root";

  private final String secretKey = "minio_pass";

  private final String region = "us-east-1";

  private AWSCredentials credentials;

  private AmazonS3 minioClient;

  private GenericContainer minioContainer;

  @Override
  public void beforeEach(ExtensionContext extensionContext) throws IllegalAccessException {
    minioContainer =
        new GenericContainer("minio/minio")
            .withExposedPorts(9000, 9001)
            .withEnv("MINIO_ROOT_USER", accessKey)
            .withEnv("MINIO_ROOT_PASSWORD", secretKey)
            .withCommand("minio server /data --console-address :9001")
            .waitingFor(Wait.forListeningPort());

    minioContainer.setPortBindings(Arrays.asList("19000:9000", "19001:9001"));

    minioContainer.start();

    credentials = new BasicAWSCredentials(accessKey, secretKey);

    minioClient =
        AmazonS3ClientBuilder.standard()
            .withClientConfiguration(new ClientConfiguration().withProtocol(Protocol.HTTP))
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(
                    String.format("%s:19000", minioContainer.getHost()), region))
            .withPathStyleAccessEnabled(true)
            .build();
    minioClient.createBucket("test-bucket");
    patchField(extensionContext, MinioClientField.class, minioClient);
  }

  @Override
  public void afterEach(ExtensionContext context) {
    minioContainer.stop();
    ;
  }
}
