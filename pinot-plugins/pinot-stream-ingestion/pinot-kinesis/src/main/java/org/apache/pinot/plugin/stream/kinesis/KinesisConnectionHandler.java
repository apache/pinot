package org.apache.pinot.plugin.stream.kinesis;

import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.v2.ConsumerV2;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;


public class KinesisConnectionHandler {
  String _awsRegion = "";
  KinesisClient _kinesisClient;

  public KinesisConnectionHandler(){

  }

  public KinesisConnectionHandler(String awsRegion){
    _awsRegion = awsRegion;
    _kinesisClient = KinesisClient.builder().region(Region.of(_awsRegion)).credentialsProvider(DefaultCredentialsProvider.create()).build();
  }

}
