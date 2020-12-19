package org.apache.pinot.plugin.stream.kinesis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.v2.Checkpoint;
import org.apache.pinot.spi.stream.v2.ConsumerV2;
import org.apache.pinot.spi.stream.v2.FetchResult;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;


public class KinesisConsumer extends KinesisConnectionHandler implements ConsumerV2 {
  String _stream;
  Integer _maxRecords;

  //TODO: Fetch AWS region from  Stream Config.
  public KinesisConsumer(String stream, String awsRegion) {
    super(stream, awsRegion);
    _stream = stream;
    _maxRecords = 20;
  }

  public KinesisConsumer(String stream, String awsRegion, StreamConfig streamConfig) {
    super(stream, awsRegion);
    _stream = stream;
    _maxRecords = Integer.parseInt(streamConfig.getStreamConfigsMap().getOrDefault("maxRecords", "20"));
  }

  @Override
  public KinesisFetchResult fetch(Checkpoint start, Checkpoint end, long timeout) {
    KinesisCheckpoint kinesisStartCheckpoint = (KinesisCheckpoint) start;

    String shardIterator = getShardIterator(kinesisStartCheckpoint);

    List<Record> recordList = new ArrayList<>();

    String kinesisEndSequenceNumber = null;

    if(end != null) {
      KinesisCheckpoint kinesisEndCheckpoint = (KinesisCheckpoint) end;
      kinesisEndSequenceNumber = kinesisEndCheckpoint.getSequenceNumber();
    }

    String nextStartSequenceNumber = null;
    Long startTimestamp = System.currentTimeMillis();

    while(shardIterator != null && !isTimedOut(startTimestamp, timeout)){
      GetRecordsRequest getRecordsRequest = GetRecordsRequest.builder().shardIterator(shardIterator).build();
      GetRecordsResponse getRecordsResponse = _kinesisClient.getRecords(getRecordsRequest);

      if(getRecordsResponse.records().size() > 0){
        recordList.addAll(getRecordsResponse.records());
        nextStartSequenceNumber = recordList.get(recordList.size() - 1).sequenceNumber();

        if(kinesisEndSequenceNumber != null && kinesisEndSequenceNumber.compareTo(recordList.get(recordList.size() - 1).sequenceNumber()) <= 0 ){
          nextStartSequenceNumber = kinesisEndSequenceNumber;
          break;
        }

        if(recordList.size() >= _maxRecords) break;
      }

      shardIterator = getRecordsResponse.nextShardIterator();
    }

    if(nextStartSequenceNumber == null && recordList.size() > 0){
      nextStartSequenceNumber = recordList.get(recordList.size() - 1).sequenceNumber();
    }

    KinesisCheckpoint kinesisCheckpoint = new KinesisCheckpoint(kinesisStartCheckpoint.getShardId(), nextStartSequenceNumber);
    KinesisFetchResult kinesisFetchResult = new KinesisFetchResult(kinesisCheckpoint,
        recordList);

    return kinesisFetchResult;
  }

  private String getShardIterator(KinesisCheckpoint kinesisStartCheckpoint) {
    GetShardIteratorResponse getShardIteratorResponse;

    if(kinesisStartCheckpoint.getSequenceNumber() != null) {
      String kinesisStartSequenceNumber = kinesisStartCheckpoint.getSequenceNumber();
      getShardIteratorResponse = _kinesisClient.getShardIterator(
          GetShardIteratorRequest.builder().streamName(_stream).shardId(kinesisStartCheckpoint.getShardId()).shardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
              .startingSequenceNumber(kinesisStartSequenceNumber).build());
    } else{
      getShardIteratorResponse = _kinesisClient.getShardIterator(
          GetShardIteratorRequest.builder().shardId(kinesisStartCheckpoint.getShardId()).streamName(_stream).shardIteratorType(ShardIteratorType.LATEST).build());
    }

    return getShardIteratorResponse.shardIterator();
  }

  private boolean isTimedOut(Long startTimestamp, Long timeout) {
    return (System.currentTimeMillis() - startTimestamp) >= timeout;
  }
}
