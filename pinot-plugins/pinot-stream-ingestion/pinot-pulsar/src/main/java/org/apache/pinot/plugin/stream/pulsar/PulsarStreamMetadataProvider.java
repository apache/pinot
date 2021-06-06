package org.apache.pinot.plugin.stream.pulsar;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;


public class PulsarStreamMetadataProvider extends PulsarPartitionLevelConnectionHandler implements StreamMetadataProvider {

  private String _clientId;
  private StreamConfig _streamConfig;
  private int _partition;

  public PulsarStreamMetadataProvider(String clientId, StreamConfig streamConfig) {
    super(clientId, streamConfig, 0);
    _clientId = clientId;
    _streamConfig = streamConfig;
  }

  public PulsarStreamMetadataProvider(String clientId, StreamConfig streamConfig, int partition) {
    super(clientId, streamConfig, partition);
    _clientId = clientId;
    _streamConfig = streamConfig;
    _partition = partition;
  }

  @Override
  public int fetchPartitionCount(long timeoutMillis) {
    try {
      return _pulsarClient.getPartitionsForTopic(_streamConfig.getTopicName()).get().size();
    } catch (Exception e){
      //TODO: Handle error
      return 0;
    }
  }

  public synchronized long fetchPartitionOffset(@Nonnull OffsetCriteria offsetCriteria, long timeoutMillis)
      throws java.util.concurrent.TimeoutException {
    throw new UnsupportedOperationException("The use of this method is not supported");
  }

  @Override
  public StreamPartitionMsgOffset fetchStreamPartitionOffset(@Nonnull OffsetCriteria offsetCriteria, long timeoutMillis)
      throws TimeoutException {
    Preconditions.checkNotNull(offsetCriteria);
    try {
      MessageId offset = null;
      if (offsetCriteria.isLargest()) {
        _reader.seek(MessageId.earliest);
        if (_reader.hasMessageAvailable()) {
          offset = _reader.readNext().getMessageId();
        }
      } else if (offsetCriteria.isSmallest()) {
        _reader.seek(MessageId.latest);
        if (_reader.hasMessageAvailable()) {
          offset = _reader.readNext().getMessageId();
        }
      } else {
        throw new IllegalArgumentException("Unknown initial offset value " + offsetCriteria.toString());
      }
      return new MessageIdStreamOffset(offset);
    } catch (PulsarClientException e){
      //TODO: handler exception
      return null;
    }
  }

  @Override
  public void close()
      throws IOException {
    //super.close();
  }
}
