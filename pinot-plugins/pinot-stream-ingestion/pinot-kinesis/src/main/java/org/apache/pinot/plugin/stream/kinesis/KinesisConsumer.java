package org.apache.pinot.plugin.stream.kinesis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

  //TODO: Fetch AWS region from  Stream Config.
  public KinesisConsumer(String stream, String awsRegion) {
    super(awsRegion);
    _stream = stream;
  }

  @Override
  public FetchResult fetch(Checkpoint start, Checkpoint end, long timeout) {
    KinesisCheckpoint kinesisStartCheckpoint = (KinesisCheckpoint) start;
    KinesisCheckpoint kinesisEndCheckpoint = (KinesisCheckpoint) end;

    String kinesisStartSequenceNumber = kinesisStartCheckpoint.getSequenceNumber();
    String kinesisEndSequenceNumber = kinesisEndCheckpoint.getSequenceNumber();

    GetShardIteratorResponse getShardIteratorResponse = _kinesisClient.getShardIterator(GetShardIteratorRequest.builder().streamName(_stream).shardIteratorType(
        ShardIteratorType.AFTER_SEQUENCE_NUMBER).startingSequenceNumber(kinesisStartSequenceNumber).build());

    String shardIterator = getShardIteratorResponse.shardIterator();
    GetRecordsRequest getRecordsRequest = GetRecordsRequest.builder().shardIterator(shardIterator).build();
    GetRecordsResponse getRecordsResponse = _kinesisClient.getRecords(getRecordsRequest);

    String kinesisNextShardIterator = getRecordsResponse.nextShardIterator();

    //TODO: Get records in the loop and stop when end sequence number is reached or there is an exception.
    if(!getRecordsResponse.hasRecords()){
      return new KinesisFetchResult(kinesisStartSequenceNumber, Collections.emptyList());
    }

    List<Record> recordList = new ArrayList<>();
    recordList.addAll(getRecordsResponse.records());

    String nextStartSequenceNumber = recordList.get(recordList.size() - 1).sequenceNumber();
    while(kinesisNextShardIterator != null){
      getRecordsRequest = GetRecordsRequest.builder().shardIterator(kinesisNextShardIterator).build();
      getRecordsResponse = _kinesisClient.getRecords(getRecordsRequest);
      if(getRecordsResponse.hasRecords()){
        recordList.addAll(getRecordsResponse.records());
        nextStartSequenceNumber = recordList.get(recordList.size() - 1).sequenceNumber();
      }

      if(kinesisEndSequenceNumber.compareTo(recordList.get(recordList.size() - 1).sequenceNumber()) <= 0 ) {
        nextStartSequenceNumber = kinesisEndSequenceNumber;
        break;
      }
      kinesisNextShardIterator = getRecordsResponse.nextShardIterator();
    }

    KinesisFetchResult kinesisFetchResult = new KinesisFetchResult(nextStartSequenceNumber,
        getRecordsResponse.records());

    return kinesisFetchResult;
  }
}
