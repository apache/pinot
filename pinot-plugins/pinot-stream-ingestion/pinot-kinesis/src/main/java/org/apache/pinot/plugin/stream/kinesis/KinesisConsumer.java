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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.stream.Checkpoint;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
  private final Logger LOG = LoggerFactory.getLogger(KinesisConsumer.class);
  String _stream;
  Integer _maxRecords;
  ExecutorService _executorService;
  ShardIteratorType _shardIteratorType;

  public KinesisConsumer(KinesisConfig kinesisConfig) {
    super(kinesisConfig.getStream(), kinesisConfig.getAwsRegion());
    _stream = kinesisConfig.getStream();
    _maxRecords = kinesisConfig.maxRecordsToFetch();
    _shardIteratorType = kinesisConfig.getShardIteratorType();
    _executorService = Executors.newSingleThreadExecutor();
  }

  /**
   * Fetch records from the Kinesis stream between the start and end KinesisCheckpoint
   */
  @Override
  public KinesisRecordsBatch fetchMessages(Checkpoint startCheckpoint, Checkpoint endCheckpoint, int timeoutMs) {
    List<Record> recordList = new ArrayList<>();
    Future<KinesisRecordsBatch> kinesisFetchResultFuture =
        _executorService.submit(() -> getResult(startCheckpoint, endCheckpoint, recordList));

    try {
      return kinesisFetchResultFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      return handleException((KinesisCheckpoint) startCheckpoint, recordList);
    }
  }

  private KinesisRecordsBatch getResult(Checkpoint start, Checkpoint end, List<Record> recordList) {
    KinesisCheckpoint kinesisStartCheckpoint = (KinesisCheckpoint) start;

    try {

      if (_kinesisClient == null) {
        createConnection();
      }

      //TODO: iterate upon all the shardIds in the map
      // Okay for now, since we have assumed that every partition group contains a single shard
      Map.Entry<String, String> next = kinesisStartCheckpoint.getShardToStartSequenceMap().entrySet().iterator().next();
      String shardIterator = getShardIterator(next.getKey(), next.getValue());

      String kinesisEndSequenceNumber = null;

      if (end != null) {
        KinesisCheckpoint kinesisEndCheckpoint = (KinesisCheckpoint) end;
        kinesisEndSequenceNumber = kinesisEndCheckpoint.getShardToStartSequenceMap().values().iterator().next();
      }

      String nextStartSequenceNumber = null;
      boolean isEndOfShard = false;

      while (shardIterator != null) {
        GetRecordsRequest getRecordsRequest = GetRecordsRequest.builder().shardIterator(shardIterator).build();
        GetRecordsResponse getRecordsResponse = _kinesisClient.getRecords(getRecordsRequest);

        if (getRecordsResponse.records().size() > 0) {
          recordList.addAll(getRecordsResponse.records());
          nextStartSequenceNumber = recordList.get(recordList.size() - 1).sequenceNumber();

          if (kinesisEndSequenceNumber != null
              && kinesisEndSequenceNumber.compareTo(recordList.get(recordList.size() - 1).sequenceNumber()) <= 0) {
            nextStartSequenceNumber = kinesisEndSequenceNumber;
            break;
          }

          if (recordList.size() >= _maxRecords) {
            break;
          }
        }

        if (getRecordsResponse.hasChildShards()) {
          //This statement returns true only when end of current shard has reached.
          isEndOfShard = true;
          break;
        }

        shardIterator = getRecordsResponse.nextShardIterator();
      }

      if (nextStartSequenceNumber == null && recordList.size() > 0) {
        nextStartSequenceNumber = recordList.get(recordList.size() - 1).sequenceNumber();
      }
      return new KinesisRecordsBatch(recordList, next.getKey(), isEndOfShard);
    } catch (IllegalStateException e) {
      LOG.warn("Illegal state exception, connection is broken", e);
      return handleException(kinesisStartCheckpoint, recordList);
    } catch (ProvisionedThroughputExceededException e) {
      LOG.warn("The request rate for the stream is too high", e);
      return handleException(kinesisStartCheckpoint, recordList);
    } catch (ExpiredIteratorException e) {
      LOG.warn("ShardIterator expired while trying to fetch records", e);
      return handleException(kinesisStartCheckpoint, recordList);
    } catch (ResourceNotFoundException | InvalidArgumentException e) {
      // aws errors
      LOG.error("Encountered AWS error while attempting to fetch records", e);
      return handleException(kinesisStartCheckpoint, recordList);
    } catch (KinesisException e) {
      LOG.warn("Encountered unknown unrecoverable AWS exception", e);
      throw new RuntimeException(e);
    } catch(IllegalStateException e){
       LOG.warn("Illegal state exception, connection is broken", e);
       return handleException(kinesisStartCheckpoint, recordList);
    } catch (Throwable e) {
      // non transient errors
      LOG.error("Unknown fetchRecords exception", e);
      throw new RuntimeException(e);
    }
  }

  private KinesisRecordsBatch handleException(KinesisCheckpoint start, List<Record> recordList) {
    String shardId = start.getShardToStartSequenceMap().entrySet().iterator().next().getKey();

    if (recordList.size() > 0) {
      String nextStartSequenceNumber = recordList.get(recordList.size() - 1).sequenceNumber();
      Map<String, String> newCheckpoint = new HashMap<>(start.getShardToStartSequenceMap());
      newCheckpoint.put(newCheckpoint.keySet().iterator().next(), nextStartSequenceNumber);
    }
    return new KinesisRecordsBatch(recordList, shardId, false);
  }

  private String getShardIterator(String shardId, String sequenceNumber) {

    GetShardIteratorRequest.Builder requestBuilder =
        GetShardIteratorRequest.builder().streamName(_stream).shardId(shardId).shardIteratorType(_shardIteratorType);

    if (sequenceNumber != null && _shardIteratorType.toString().contains("SEQUENCE")) {
      requestBuilder = requestBuilder.startingSequenceNumber(sequenceNumber);
    }

    return _kinesisClient.getShardIterator(requestBuilder.build()).shardIterator();
  }

  @Override
  public void close() {
    super.close();
  }
}
