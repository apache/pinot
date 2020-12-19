package org.apache.pinot.plugin.stream.kinesis;

import java.util.List;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.v2.ConsumerV2;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;


public class KinesisConnectionHandler {
  private String _stream;
  private String _awsRegion;
  KinesisClient _kinesisClient;

  public KinesisConnectionHandler(){

  }

  public KinesisConnectionHandler(String stream, String awsRegion){
    _stream = stream;
    _awsRegion = awsRegion;
    _kinesisClient = KinesisClient.builder().region(Region.of(_awsRegion)).credentialsProvider(DefaultCredentialsProvider.create()).build();
  }

  public List<Shard> getShards(){
    ListShardsResponse listShardsResponse =  _kinesisClient.listShards(ListShardsRequest.builder().streamName(_stream).build());
    return listShardsResponse.shards();
  }

}
