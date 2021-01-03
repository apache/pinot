package org.apache.pinot.plugin.stream.kinesis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.PartitionGroupInfo;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import software.amazon.awssdk.services.kinesis.model.Shard;


public class KinesisStreamMetadataProvider implements StreamMetadataProvider {
  private final KinesisConfig _kinesisConfig;
  private KinesisConnectionHandler _kinesisConnectionHandler;

  public KinesisStreamMetadataProvider(String clientId, KinesisConfig kinesisConfig) {
    _kinesisConfig = kinesisConfig;
    _kinesisConnectionHandler = new KinesisConnectionHandler(kinesisConfig.getStream(), kinesisConfig.getAwsRegion());
  }

  @Override
  public int fetchPartitionCount(long timeoutMillis) {
    return 0;
  }

  @Override
  public long fetchPartitionOffset(@Nonnull OffsetCriteria offsetCriteria, long timeoutMillis)
      throws TimeoutException {
    return 0;
  }

  @Override
  public List<PartitionGroupInfo> getPartitionGroupInfoList(String clientId, StreamConfig streamConfig,
      List<PartitionGroupMetadata> currentPartitionGroupsMetadata, int timeoutMillis)
      throws TimeoutException {
    List<PartitionGroupInfo> partitionGroupInfos = new ArrayList<>();
    List<Shard> shards = _kinesisConnectionHandler.getShards();
    for (Shard shard : shards) {
      partitionGroupInfos.add(new PartitionGroupInfo(shard.shardId().hashCode(), shard.sequenceNumberRange().startingSequenceNumber()));
    }
    return partitionGroupInfos;
  }

  @Override
  public void close()
      throws IOException {

  }
}
