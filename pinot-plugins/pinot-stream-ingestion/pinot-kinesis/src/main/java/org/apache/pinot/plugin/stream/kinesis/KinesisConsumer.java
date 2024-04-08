/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.plugin.stream.kinesis;

import com.google.common.annotations.VisibleForTesting;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.spi.stream.BytesStreamMessage;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.InvalidArgumentException;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;


/**
 * A {@link PartitionGroupConsumer} implementation for the Kinesis stream
 */
public class KinesisConsumer extends KinesisConnectionHandler implements PartitionGroupConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(KinesisConsumer.class);
  private static final long SLEEP_TIME_BETWEEN_REQUESTS = 1000L;

  // TODO: Revisit the logic of using a separate executor to manage the request timeout. Currently it is not thread safe
  private final ExecutorService _executorService = Executors.newSingleThreadExecutor();

  public KinesisConsumer(KinesisConfig config) {
    super(config);
    LOGGER.info("Created Kinesis consumer with topic: {}, RPS limit: {}, max records per fetch: {}",
        config.getStreamTopicName(), config.getRpsLimit(), config.getNumMaxRecordsToFetch());
  }

  @VisibleForTesting
  public KinesisConsumer(KinesisConfig config, KinesisClient kinesisClient) {
    super(config, kinesisClient);
  }

  /**
   * Fetch records from the Kinesis stream between the start and end KinesisCheckpoint
   */
  @Override
  public KinesisMessageBatch fetchMessages(StreamPartitionMsgOffset startMsgOffset, int timeoutMs) {
    KinesisPartitionGroupOffset startOffset = (KinesisPartitionGroupOffset) startMsgOffset;
    List<BytesStreamMessage> messages = new ArrayList<>();
    Future<KinesisMessageBatch> kinesisFetchResultFuture =
        _executorService.submit(() -> getResult(startOffset, messages));
    try {
      return kinesisFetchResultFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      kinesisFetchResultFuture.cancel(true);
    } catch (Exception e) {
      // Ignored
    }
    return buildKinesisMessageBatch(startOffset, messages, false);
  }

  private KinesisMessageBatch getResult(KinesisPartitionGroupOffset startOffset, List<BytesStreamMessage> messages) {
    try {
      String shardId = startOffset.getShardId();
      String shardIterator = getShardIterator(shardId, startOffset.getSequenceNumber());
      boolean endOfShard = false;
      long currentWindow = System.currentTimeMillis() / SLEEP_TIME_BETWEEN_REQUESTS;
      int currentWindowRequests = 0;
      while (shardIterator != null) {
        GetRecordsRequest getRecordsRequest = GetRecordsRequest.builder().shardIterator(shardIterator).build();
        long requestSentTime = System.currentTimeMillis() / 1000;
        GetRecordsResponse getRecordsResponse = _kinesisClient.getRecords(getRecordsRequest);
        List<Record> records = getRecordsResponse.records();
        if (!records.isEmpty()) {
          for (Record record : records) {
            messages.add(extractStreamMessage(record, shardId));
          }
          if (messages.size() >= _config.getNumMaxRecordsToFetch()) {
            break;
          }
        }

        if (getRecordsResponse.hasChildShards() && !getRecordsResponse.childShards().isEmpty()) {
          //This statement returns true only when end of current shard has reached.
          // hasChildShards only checks if the childShard is null and is a valid instance.
          endOfShard = true;
          break;
        }

        shardIterator = getRecordsResponse.nextShardIterator();

        if (Thread.interrupted()) {
          break;
        }

        // Kinesis enforces a limit of 5 .getRecords request per second on each shard from AWS end
        // Beyond this limit we start getting ProvisionedThroughputExceededException which affect the ingestion
        if (requestSentTime == currentWindow) {
          currentWindowRequests++;
        } else if (requestSentTime > currentWindow) {
          currentWindow = requestSentTime;
          currentWindowRequests = 0;
        }

        if (currentWindowRequests >= _config.getNumMaxRecordsToFetch()) {
          try {
            Thread.sleep(SLEEP_TIME_BETWEEN_REQUESTS);
          } catch (InterruptedException e) {
            LOGGER.debug("Sleep interrupted while rate limiting Kinesis requests", e);
            break;
          }
        }
      }

      return buildKinesisMessageBatch(startOffset, messages, endOfShard);
    } catch (IllegalStateException e) {
      debugOrLogWarning("Illegal state exception, connection is broken", e);
    } catch (ProvisionedThroughputExceededException e) {
      debugOrLogWarning("The request rate for the stream is too high", e);
    } catch (ExpiredIteratorException e) {
      debugOrLogWarning("ShardIterator expired while trying to fetch records", e);
    } catch (ResourceNotFoundException | InvalidArgumentException e) {
      // aws errors
      LOGGER.error("Encountered AWS error while attempting to fetch records", e);
    } catch (KinesisException e) {
      debugOrLogWarning("Encountered unknown unrecoverable AWS exception", e);
      throw new RuntimeException(e);
    } catch (AbortedException e) {
      if (!(e.getCause() instanceof InterruptedException)) {
        debugOrLogWarning("Task aborted due to exception", e);
      }
    } catch (Throwable e) {
      // non transient errors
      LOGGER.error("Unknown fetchRecords exception", e);
      throw new RuntimeException(e);
    }
    return buildKinesisMessageBatch(startOffset, messages, false);
  }

  private void debugOrLogWarning(String message, Throwable throwable) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(message, throwable);
    } else {
      LOGGER.warn(message + ": " + throwable.getMessage());
    }
  }

  private KinesisMessageBatch buildKinesisMessageBatch(KinesisPartitionGroupOffset startOffset,
      List<BytesStreamMessage> messages, boolean endOfShard) {
    KinesisPartitionGroupOffset offsetOfNextBatch;
    if (messages.isEmpty()) {
      offsetOfNextBatch = startOffset;
    } else {
      StreamMessageMetadata lastMessageMetadata = messages.get(messages.size() - 1).getMetadata();
      assert lastMessageMetadata != null;
      offsetOfNextBatch = (KinesisPartitionGroupOffset) lastMessageMetadata.getNextOffset();
    }
    return new KinesisMessageBatch(messages, offsetOfNextBatch, endOfShard);
  }

  private String getShardIterator(String shardId, String sequenceNumber) {
    GetShardIteratorRequest.Builder requestBuilder =
        GetShardIteratorRequest.builder().streamName(_config.getStreamTopicName()).shardId(shardId);
    if (sequenceNumber != null) {
      requestBuilder = requestBuilder.startingSequenceNumber(sequenceNumber)
          .shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
    } else {
      requestBuilder = requestBuilder.shardIteratorType(_config.getShardIteratorType());
    }
    return _kinesisClient.getShardIterator(requestBuilder.build()).shardIterator();
  }

  private BytesStreamMessage extractStreamMessage(Record record, String shardId) {
    byte[] key = record.partitionKey().getBytes(StandardCharsets.UTF_8);
    byte[] value = record.data().asByteArray();
    long timestamp = record.approximateArrivalTimestamp().toEpochMilli();
    String sequenceNumber = record.sequenceNumber();
    KinesisPartitionGroupOffset offset = new KinesisPartitionGroupOffset(shardId, sequenceNumber);
    // NOTE: Use the same offset as next offset because the consumer starts consuming AFTER the start sequence number.
    StreamMessageMetadata.Builder builder =
        new StreamMessageMetadata.Builder().setRecordIngestionTimeMs(timestamp).setOffset(offset, offset);
    if (_config.isPopulateMetadata()) {
      builder.setMetadata(Map.of(KinesisStreamMessageMetadata.APPRX_ARRIVAL_TIMESTAMP_KEY, String.valueOf(timestamp),
          KinesisStreamMessageMetadata.SEQUENCE_NUMBER_KEY, sequenceNumber));
    }
    StreamMessageMetadata metadata = builder.build();
    return new BytesStreamMessage(key, value, metadata);
  }

  @Override
  public void close() {
    super.close();
    shutdownAndAwaitTermination();
  }

  void shutdownAndAwaitTermination() {
    _executorService.shutdown();
    try {
      if (!_executorService.awaitTermination(60, TimeUnit.SECONDS)) {
        _executorService.shutdownNow();
      }
    } catch (InterruptedException ie) {
      _executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
