package org.apache.pinot.plugin.stream.pulsar;

import java.util.Set;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.PartitionLevelConsumer;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamLevelConsumer;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffsetFactory;


public class PulsarConsumerFactory extends StreamConsumerFactory {
  @Override
  public PartitionLevelConsumer createPartitionLevelConsumer(String clientId, int partition) {
    return new PulsarPartitionLevelConsumer(clientId, _streamConfig, partition);
  }

  @Override
  public StreamLevelConsumer createStreamLevelConsumer(String clientId, String tableName, Set<String> fieldsToRead,
      String groupId) {
    return new PulsarStreamLevelConsumer(clientId, tableName, _streamConfig, fieldsToRead, groupId);
  }

  @Override
  public StreamMetadataProvider createPartitionMetadataProvider(String clientId, int partition) {
    return new PulsarStreamMetadataProvider(clientId, _streamConfig, partition);
  }

  @Override
  public StreamMetadataProvider createStreamMetadataProvider(String clientId) {
    return new PulsarStreamMetadataProvider(clientId, _streamConfig);
  }

  @Override
  public StreamPartitionMsgOffsetFactory createStreamMsgOffsetFactory() {
    return new MessageIdStreamOffsetFactory();
  }

  @Override
  public PartitionGroupConsumer createPartitionGroupConsumer(String clientId,
      PartitionGroupConsumptionStatus partitionGroupConsumptionStatus) {
    throw new UnsupportedOperationException();
  }
}
