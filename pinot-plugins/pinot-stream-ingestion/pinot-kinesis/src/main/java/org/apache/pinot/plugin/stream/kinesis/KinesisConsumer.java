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
import com.google.common.base.Preconditions;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
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
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;


/**
 * A {@link PartitionGroupConsumer} implementation for the Kinesis stream
 */
public class KinesisConsumer extends KinesisConnectionHandler implements PartitionGroupConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(KinesisConsumer.class);
  public static final long SLEEP_TIME_BETWEEN_REQUESTS = 1000L;
  private final String _streamTopicName;
  private final int _numMaxRecordsToFetch;
  private final ExecutorService _executorService;
  private final ShardIteratorType _shardIteratorType;
  private final int _rpsLimit;

  public KinesisConsumer(KinesisConfig kinesisConfig) {
    super(kinesisConfig);
    _streamTopicName = kinesisConfig.getStreamTopicName();
    _numMaxRecordsToFetch = kinesisConfig.getNumMaxRecordsToFetch();
    _shardIteratorType = kinesisConfig.getShardIteratorType();
    _rpsLimit = kinesisConfig.getRpsLimit();
    _executorService = Executors.newSingleThreadExecutor();
  }

  @VisibleForTesting
  public KinesisConsumer(KinesisConfig kinesisConfig, KinesisClient kinesisClient) {
    super(kinesisConfig, kinesisClient);
    _kinesisClient = kinesisClient;
    _streamTopicName = kinesisConfig.getStreamTopicName();
    _numMaxRecordsToFetch = kinesisConfig.getNumMaxRecordsToFetch();
    _shardIteratorType = kinesisConfig.getShardIteratorType();
    _rpsLimit = kinesisConfig.getRpsLimit();
    _executorService = Executors.newSingleThreadExecutor();
  }

  /**
   * Fetch records from the Kinesis stream between the start and end KinesisCheckpoint
   */
  @Override
  public KinesisRecordsBatch fetchMessages(StreamPartitionMsgOffset startCheckpoint,
      StreamPartitionMsgOffset endCheckpoint, int timeoutMs) {
    List<KinesisStreamMessage> recordList = new ArrayList<>();
    Future<KinesisRecordsBatch> kinesisFetchResultFuture =
        _executorService.submit(() -> getResult(startCheckpoint, endCheckpoint, recordList));

    try {
      return kinesisFetchResultFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      kinesisFetchResultFuture.cancel(true);
      return handleException((KinesisPartitionGroupOffset) startCheckpoint, recordList);
    } catch (Exception e) {
      return handleException((KinesisPartitionGroupOffset) startCheckpoint, recordList);
    }
  }

  private KinesisRecordsBatch getResult(StreamPartitionMsgOffset startOffset, StreamPartitionMsgOffset endOffset,
      List<KinesisStreamMessage> recordList) {
    KinesisPartitionGroupOffset kinesisStartCheckpoint = (KinesisPartitionGroupOffset) startOffset;

    try {
      if (_kinesisClient == null) {
        createConnection();
      }

      // TODO: iterate upon all the shardIds in the map
      //  Okay for now, since we have assumed that every partition group contains a single shard
      Map<String, String> startShardToSequenceMap = kinesisStartCheckpoint.getShardToStartSequenceMap();
      Preconditions.checkState(startShardToSequenceMap.size() == 1,
          "Only 1 shard per consumer supported. Found: %s, in startShardToSequenceMap",
          startShardToSequenceMap.keySet());
      Map.Entry<String, String> startShardToSequenceNum = startShardToSequenceMap.entrySet().iterator().next();
      String shardIterator = getShardIterator(startShardToSequenceNum.getKey(), startShardToSequenceNum.getValue());

      String kinesisEndSequenceNumber = null;

      if (endOffset != null) {
        KinesisPartitionGroupOffset kinesisEndCheckpoint = (KinesisPartitionGroupOffset) endOffset;
        Map<String, String> endShardToSequenceMap = kinesisEndCheckpoint.getShardToStartSequenceMap();
        Preconditions.checkState(endShardToSequenceMap.size() == 1,
            "Only 1 shard per consumer supported. Found: %s, in endShardToSequenceMap", endShardToSequenceMap.keySet());
        kinesisEndSequenceNumber = endShardToSequenceMap.values().iterator().next();
      }

      String nextStartSequenceNumber;
      boolean isEndOfShard = false;
      long currentWindow = System.currentTimeMillis() / SLEEP_TIME_BETWEEN_REQUESTS;
      int currentWindowRequests = 0;
      while (shardIterator != null) {
        GetRecordsRequest getRecordsRequest = GetRecordsRequest.builder().shardIterator(shardIterator).build();

        long requestSentTime = System.currentTimeMillis() / 1000;
        GetRecordsResponse getRecordsResponse = _kinesisClient.getRecords(getRecordsRequest);

        if (!getRecordsResponse.records().isEmpty()) {
          getRecordsResponse.records().forEach(r -> {
            recordList.add(
            new KinesisStreamMessage(r.partitionKey().getBytes(StandardCharsets.UTF_8), r.data().asByteArray(),
                r.sequenceNumber(), (KinesisStreamMessageMetadata) _kinesisMetadataExtractor.extract(r),
                r.data().asByteArray().length));
          });
          nextStartSequenceNumber = recordList.get(recordList.size() - 1).sequenceNumber();

          if (kinesisEndSequenceNumber != null && kinesisEndSequenceNumber.compareTo(nextStartSequenceNumber) <= 0) {
            break;
          }

          if (recordList.size() >= _numMaxRecordsToFetch) {
            break;
          }
        }

        if (getRecordsResponse.hasChildShards() && !getRecordsResponse.childShards().isEmpty()) {
          //This statement returns true only when end of current shard has reached.
          // hasChildShards only checks if the childShard is null and is a valid instance.
          isEndOfShard = true;
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

        if (currentWindowRequests >= _rpsLimit) {
          try {
            Thread.sleep(SLEEP_TIME_BETWEEN_REQUESTS);
          } catch (InterruptedException e) {
            LOGGER.debug("Sleep interrupted while rate limiting Kinesis requests", e);
            break;
          }
        }
      }

      return new KinesisRecordsBatch(recordList, startShardToSequenceNum.getKey(), isEndOfShard);
    } catch (IllegalStateException e) {
      debugOrLogWarning("Illegal state exception, connection is broken", e);
      return handleException(kinesisStartCheckpoint, recordList);
    } catch (ProvisionedThroughputExceededException e) {
      debugOrLogWarning("The request rate for the stream is too high", e);
      return handleException(kinesisStartCheckpoint, recordList);
    } catch (ExpiredIteratorException e) {
      debugOrLogWarning("ShardIterator expired while trying to fetch records", e);
      return handleException(kinesisStartCheckpoint, recordList);
    } catch (ResourceNotFoundException | InvalidArgumentException e) {
      // aws errors
      LOGGER.error("Encountered AWS error while attempting to fetch records", e);
      return handleException(kinesisStartCheckpoint, recordList);
    } catch (KinesisException e) {
      debugOrLogWarning("Encountered unknown unrecoverable AWS exception", e);
      throw new RuntimeException(e);
    } catch (AbortedException e) {
      if (!(e.getCause() instanceof InterruptedException)) {
        debugOrLogWarning("Task aborted due to exception", e);
      }
      return handleException(kinesisStartCheckpoint, recordList);
    } catch (Throwable e) {
      // non transient errors
      LOGGER.error("Unknown fetchRecords exception", e);
      throw new RuntimeException(e);
    }
  }

  private void debugOrLogWarning(String message, Throwable throwable) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(message, throwable);
    } else {
      LOGGER.warn(message + ": " + throwable.getMessage());
    }
  }

  private KinesisRecordsBatch handleException(KinesisPartitionGroupOffset start,
      List<KinesisStreamMessage> recordList) {
    String shardId = start.getShardToStartSequenceMap().entrySet().iterator().next().getKey();

    if (!recordList.isEmpty()) {
      String nextStartSequenceNumber = recordList.get(recordList.size() - 1).sequenceNumber();
      Map<String, String> newCheckpoint = new HashMap<>(start.getShardToStartSequenceMap());
      newCheckpoint.put(newCheckpoint.keySet().iterator().next(), nextStartSequenceNumber);
    }
    return new KinesisRecordsBatch(recordList, shardId, false);
  }

  private String getShardIterator(String shardId, String sequenceNumber) {
    GetShardIteratorRequest.Builder requestBuilder =
        GetShardIteratorRequest.builder().streamName(_streamTopicName).shardId(shardId);

    if (sequenceNumber != null) {
      requestBuilder = requestBuilder.startingSequenceNumber(sequenceNumber)
          .shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
    } else {
      requestBuilder = requestBuilder.shardIteratorType(_shardIteratorType);
    }

    return _kinesisClient.getShardIterator(requestBuilder.build()).shardIterator();
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
