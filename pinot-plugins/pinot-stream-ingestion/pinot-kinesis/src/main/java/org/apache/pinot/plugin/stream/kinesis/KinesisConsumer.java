package org.apache.pinot.plugin.stream.kinesis;

import java.util.Collections;
import org.apache.pinot.spi.stream.v2.Checkpoint;
import org.apache.pinot.spi.stream.v2.ConsumerV2;
import org.apache.pinot.spi.stream.v2.FetchResult;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;


public class KinesisConsumer extends KinesisConnectionHandler implements ConsumerV2 {

  //TODO: Fetch AWS region from  Stream Config.
  public KinesisConsumer(String awsRegion) {
    super(awsRegion);
  }

  @Override
  public FetchResult fetch(Checkpoint start, Checkpoint end, long timeout) {
    KinesisCheckpoint kinesisStartCheckpoint = (KinesisCheckpoint) start;
    KinesisCheckpoint kinesisEndCheckpoint = (KinesisCheckpoint) end;

    String kinesisShardIteratorStart = kinesisStartCheckpoint.getShardIterator();

    GetRecordsRequest getRecordsRequest = GetRecordsRequest.builder().shardIterator(kinesisShardIteratorStart).build();
    GetRecordsResponse getRecordsResponse = _kinesisClient.getRecords(getRecordsRequest);

    String kinesisNextShardIterator = getRecordsResponse.nextShardIterator();

    if(!getRecordsResponse.hasRecords()){
      return new KinesisFetchResult(kinesisNextShardIterator, Collections.emptyList());
    }

    KinesisFetchResult kinesisFetchResult = new KinesisFetchResult(kinesisNextShardIterator,
        getRecordsResponse.records());

    return kinesisFetchResult;
  }
}
