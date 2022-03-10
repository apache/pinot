package org.apache.pinot.plugin.stream.kinesis.server;

import java.net.URI;
import java.util.Properties;
import java.util.UUID;
import org.apache.pinot.spi.stream.StreamDataProducer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.apache.ApacheSdkHttpService;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.utils.AttributeMap;


public class KinesisDataProducer implements StreamDataProducer {
  public static final String ENDPOINT = "endpoint";
  public static final String REGION = "region";
  public static final String ACCESS = "access";
  public static final String SECRET = "secret";
  public static final String DEFAULT_PORT = "4566";

  private KinesisClient _kinesisClient;
  private String _localStackKinesisEndpoint = "http://localhost:%s";

  @Override
  public void init(Properties props) {
    try {
      _localStackKinesisEndpoint = String.format(_localStackKinesisEndpoint, props.getProperty("port", DEFAULT_PORT));
      _kinesisClient = KinesisClient.builder().httpClient(new ApacheSdkHttpService().createHttpClientBuilder().buildWithDefaults(
              AttributeMap.builder().put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, Boolean.TRUE).build())).credentialsProvider(getLocalAWSCredentials(props)).region(Region.of(props.getProperty(
              REGION)))
          .endpointOverride(new URI(_localStackKinesisEndpoint)).build();
    } catch (Exception e){
      _kinesisClient = null;
    }
  }

  @Override
  public void produce(String topic, byte[] payload) {
    PutRecordRequest putRecordRequest =
        PutRecordRequest.builder().streamName(topic).data(SdkBytes.fromByteArray(payload))
            .partitionKey(UUID.randomUUID().toString()).build();
    PutRecordResponse putRecordResponse = _kinesisClient.putRecord(putRecordRequest);
  }

  @Override
  public void produce(String topic, byte[] key, byte[] payload) {
    PutRecordRequest putRecordRequest =
        PutRecordRequest.builder().streamName(topic).data(SdkBytes.fromByteArray(payload))
            .partitionKey(new String(key)).build();
    PutRecordResponse putRecordResponse = _kinesisClient.putRecord(putRecordRequest);
  }

  @Override
  public void close() {
    _kinesisClient.close();
  }

  private AwsCredentialsProvider getLocalAWSCredentials(Properties props) {
    return StaticCredentialsProvider.create(AwsBasicCredentials.create(props.getProperty(ACCESS), props.getProperty(
        SECRET)));
  }
}
