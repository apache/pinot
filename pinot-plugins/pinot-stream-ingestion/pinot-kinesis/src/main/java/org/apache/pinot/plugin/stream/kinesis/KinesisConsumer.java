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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.v2.Checkpoint;
import org.apache.pinot.spi.stream.v2.ConsumerV2;
import org.apache.pinot.spi.stream.v2.FetchResult;
import org.apache.pinot.spi.stream.v2.PartitionGroupMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.InvalidArgumentException;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

public class KinesisConsumer extends KinesisConnectionHandler implements ConsumerV2 {
  String _stream;
  Integer _maxRecords;
  String _shardId;
  ExecutorService _executorService;
  private final Logger LOG = LoggerFactory.getLogger(KinesisConsumer.class);

  public KinesisConsumer(KinesisConfig kinesisConfig, PartitionGroupMetadata partitionGroupMetadata) {
    super(kinesisConfig.getStream(), kinesisConfig.getAwsRegion());
    _stream = kinesisConfig.getStream();
    _maxRecords = kinesisConfig.maxRecordsToFetch();
    KinesisShardMetadata kinesisShardMetadata = (KinesisShardMetadata) partitionGroupMetadata;
    _shardId = kinesisShardMetadata.getShardId();
    _executorService = Executors.newSingleThreadExecutor();
  }

  @Override
  public KinesisFetchResult fetch(Checkpoint start, Checkpoint end, long timeout) {
    Future<KinesisFetchResult> kinesisFetchResultFuture = _executorService.submit(() -> getResult(start, end));

    try {
      return kinesisFetchResultFuture.get(timeout, TimeUnit.MILLISECONDS);
    } catch(Exception e){
      return null;
    }
  }

  private KinesisFetchResult getResult(Checkpoint start, Checkpoint end) {
    List<Record> recordList = new ArrayList<>();
    KinesisCheckpoint kinesisStartCheckpoint = (KinesisCheckpoint) start;

    try {

      String shardIterator = getShardIterator(kinesisStartCheckpoint);

      String kinesisEndSequenceNumber = null;

      if (end != null) {
        KinesisCheckpoint kinesisEndCheckpoint = (KinesisCheckpoint) end;
        kinesisEndSequenceNumber = kinesisEndCheckpoint.getSequenceNumber();
      }

      String nextStartSequenceNumber = null;

      while (shardIterator != null) {
        GetRecordsRequest getRecordsRequest = GetRecordsRequest.builder().shardIterator(shardIterator).build();
        GetRecordsResponse getRecordsResponse = _kinesisClient.getRecords(getRecordsRequest);

        if (getRecordsResponse.records().size() > 0) {
          recordList.addAll(getRecordsResponse.records());
          nextStartSequenceNumber = recordList.get(recordList.size() - 1).sequenceNumber();

          if (kinesisEndSequenceNumber != null && kinesisEndSequenceNumber.compareTo(recordList.get(recordList.size() - 1).sequenceNumber()) <= 0) {
            nextStartSequenceNumber = kinesisEndSequenceNumber;
            break;
          }

          if (recordList.size() >= _maxRecords) {
            break;
          }
        }

        shardIterator = getRecordsResponse.nextShardIterator();
      }

      if (nextStartSequenceNumber == null && recordList.size() > 0) {
        nextStartSequenceNumber = recordList.get(recordList.size() - 1).sequenceNumber();
      }

      KinesisCheckpoint kinesisCheckpoint = new KinesisCheckpoint(nextStartSequenceNumber);
      KinesisFetchResult kinesisFetchResult = new KinesisFetchResult(kinesisCheckpoint, recordList);

      return kinesisFetchResult;
    }catch (ProvisionedThroughputExceededException e) {
      LOG.warn(
          "The request rate for the stream is too high"
      , e);
      return handleException(kinesisStartCheckpoint, recordList);
    }
    catch (ExpiredIteratorException e) {
      LOG.warn(
          "ShardIterator expired while trying to fetch records",e
      );
      return handleException(kinesisStartCheckpoint, recordList);
    }
    catch (ResourceNotFoundException | InvalidArgumentException e) {
      // aws errors
      LOG.error("Encountered AWS error while attempting to fetch records", e);
      return handleException(kinesisStartCheckpoint, recordList);
    }
    catch (KinesisException e) {
      LOG.warn("Encountered unknown unrecoverable AWS exception", e);
      throw new RuntimeException(e);
    }
    catch (Throwable e) {
      // non transient errors
      LOG.error("Unknown fetchRecords exception", e);
      throw new RuntimeException(e);
    }
  }

  private KinesisFetchResult handleException(KinesisCheckpoint start, List<Record> recordList) {
    if(recordList.size() > 0){
      String nextStartSequenceNumber = recordList.get(recordList.size() - 1).sequenceNumber();
      KinesisCheckpoint kinesisCheckpoint = new KinesisCheckpoint(nextStartSequenceNumber);
      return new KinesisFetchResult(kinesisCheckpoint, recordList);
    }else{
      KinesisCheckpoint kinesisCheckpoint = new KinesisCheckpoint(start.getSequenceNumber());
      return new KinesisFetchResult(kinesisCheckpoint, recordList);
    }
  }

  private String getShardIterator(KinesisCheckpoint kinesisStartCheckpoint) {
    GetShardIteratorResponse getShardIteratorResponse;

    if (kinesisStartCheckpoint.getSequenceNumber() != null) {
      String kinesisStartSequenceNumber = kinesisStartCheckpoint.getSequenceNumber();
      getShardIteratorResponse = _kinesisClient.getShardIterator(
          GetShardIteratorRequest.builder().streamName(_stream).shardId(_shardId)
              .shardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
              .startingSequenceNumber(kinesisStartSequenceNumber).build());
    } else {
      getShardIteratorResponse = _kinesisClient.getShardIterator(
          GetShardIteratorRequest.builder().shardId(_shardId).streamName(_stream)
              .shardIteratorType(ShardIteratorType.LATEST).build());
    }

    return getShardIteratorResponse.shardIterator();
  }

}
