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
package org.apache.pinot.plugin.stream.microbatch.kafka30;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderFileConfig;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.StreamMessage;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Reads batch files and converts them into {@link MessageBatch} objects.
 *
 * <p>This utility class handles the conversion of downloaded batch files (Avro, Parquet, JSON)
 * into streams of {@link GenericRow} records wrapped in {@link MessageBatch} containers. It uses
 * Pinot's {@link RecordReader} infrastructure to parse files and generates composite offsets
 * for each record to enable mid-batch resume.
 *
 * <p>Key features:
 * <ul>
 *   <li>Lazy iteration: Records are read on-demand, not loaded entirely into memory</li>
 *   <li>Mid-batch resume: Supports skipping records for restart after segment commit</li>
 *   <li>Batch sizing: Groups records into configurable batch sizes (default 500)</li>
 *   <li>Offset tracking: Each record gets a composite offset (Kafka offset + record index)</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>
 * try (CloseableIterator&lt;MessageBatch&lt;GenericRow&gt;&gt; iterator =
 *     MessageBatchReader.read(microBatch, dataFile, batchSize, totalRecords, recordsToSkip)) {
 *   while (iterator.hasNext()) {
 *     MessageBatch&lt;GenericRow&gt; batch = iterator.next();
 *     // process batch
 *   }
 * }
 * </pre>
 *
 * @see MicroBatchQueueManager which uses this class to process downloaded files
 */
public final class MessageBatchReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(MessageBatchReader.class);

  private MessageBatchReader() {
  }

  public static CloseableIterator<MessageBatch<GenericRow>> read(
      MicroBatch microBatch,
      File dataFile,
      int messageBatchSize,
      int totalRecordCount,
      int recordsToSkip) {
    try {
      FileFormat fileFormat =
          convertProtocolFormatToFileFormat(microBatch.getProtocol().getFormat());
      RecordReaderFileConfig recordReaderFileConfig =
          new RecordReaderFileConfig(fileFormat, dataFile, null, null, null);
      RecordReader recordReader = recordReaderFileConfig.getRecordReader();
      return new LazyMessageBatchIterator(
          microBatch,
          dataFile.length(),
          recordReader,
          messageBatchSize,
          totalRecordCount,
          recordsToSkip);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse records from file: " + dataFile, e);
    }
  }

  private static class LazyMessageBatchIterator
      implements CloseableIterator<MessageBatch<GenericRow>> {
    private final MicroBatch _microBatch;
    private final long _dataSize;
    private final RecordReader _recordReader;
    private final int _messageBatchSize;
    private final int _totalRecordCount;
    private final StreamMessageMetadata _microBatchMetadata;
    private final int _valueSize;

    private int _currentRecordIndex;
    private boolean _closed;

    public LazyMessageBatchIterator(
        MicroBatch microBatch,
        long dataSize,
        RecordReader recordReader,
        int messageBatchSize,
        int totalRecordCount,
        int recordsToSkip) {
      _microBatch = microBatch;
      _dataSize = dataSize;
      _recordReader = recordReader;
      _messageBatchSize = messageBatchSize;
      _totalRecordCount = totalRecordCount;
      _microBatchMetadata = microBatch.getMessageMetadata();

      _valueSize = Math.toIntExact(_totalRecordCount == 0 ? 0 : _dataSize / _totalRecordCount);
      _closed = false;

      // Skip records for mid-batch resume
      _currentRecordIndex = skipRecords(recordsToSkip);
    }

    /**
     * Skip the specified number of records in the file.
     * @return the actual number of records skipped (may be less if file has fewer records)
     */
    private int skipRecords(int recordsToSkip) {
      try {
        int skipped = 0;
        while (skipped < recordsToSkip && _recordReader.hasNext()) {
          _recordReader.next();  // Read and discard
          skipped++;
        }
        return skipped;
      } catch (Exception e) {
        throw new RuntimeException("Failed to skip records", e);
      }
    }

    @Override
    public boolean hasNext() {
      return !_closed && _currentRecordIndex < _totalRecordCount && _recordReader.hasNext();
    }

    @Override
    public MessageBatch next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more message batches available");
      }

      try {
        int startIndex = _currentRecordIndex;
        int batchSize = Math.min(_messageBatchSize, _totalRecordCount - _currentRecordIndex);

        List<StreamMessage<GenericRow>> streamMessageList = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize && _recordReader.hasNext(); i++) {
          GenericRow row = _recordReader.next();
          int recordIndex = startIndex + i;

          long kafkaMessageOffset =
              ((MicroBatchStreamPartitionMsgOffset) _microBatchMetadata.getOffset()).getKafkaMessageOffset();
          long nextKafkaMessageOffset =
              ((MicroBatchStreamPartitionMsgOffset) _microBatchMetadata.getNextOffset()).getKafkaMessageOffset();

          StreamPartitionMsgOffset msgOffset =
              new MicroBatchStreamPartitionMsgOffset(kafkaMessageOffset, recordIndex);
          StreamPartitionMsgOffset nextOffset;
          if (recordIndex == _totalRecordCount - 1) {
            nextOffset = new MicroBatchStreamPartitionMsgOffset(nextKafkaMessageOffset, 0);
          } else {
            nextOffset =
                new MicroBatchStreamPartitionMsgOffset(kafkaMessageOffset, recordIndex + 1);
          }

          StreamMessageMetadata metadata =
              new StreamMessageMetadata.Builder()
                  .setRecordIngestionTimeMs(_microBatchMetadata.getRecordIngestionTimeMs())
                  .setFirstStreamRecordIngestionTimeMs(
                      _microBatchMetadata.getFirstStreamRecordIngestionTimeMs())
                  .setOffset(msgOffset, nextOffset)
                  .setSerializedValueSize(_valueSize)
                  .setHeaders(_microBatchMetadata.getHeaders())
                  .setMetadata(_microBatchMetadata.getRecordMetadata())
                  .build();

          streamMessageList.add(
              new StreamMessage<>(_microBatch.getKey(), row, _valueSize, metadata));
          _currentRecordIndex++;
        }

        return new GenericRowMessageBatch(
            streamMessageList,
            _microBatch.getUnfilteredMessageCount(),
            _microBatch.hasDataLoss(),
            (long) streamMessageList.size() * _valueSize,
            streamMessageList.get(streamMessageList.size() - 1).getMetadata().getNextOffset());
      } catch (Exception e) {
        throw new RuntimeException("Failed to build message batch", e);
      }
    }

    @Override
    public void close() {
      if (_closed) {
        return;
      }
      try {
        // Validate that we didn't encounter more records than expected
        if (_recordReader.hasNext()) {
          LOGGER.error("RecordReader has more records than expected totalRecordCount={}. "
              + "Processed {} records so far. This indicates a mismatch between numRecords in the "
              + "protocol message and actual file contents.", _totalRecordCount, _currentRecordIndex);
        }
        _recordReader.close();
      } catch (IOException e) {
        throw new RuntimeException("Failed to close record reader", e);
      } finally {
        _closed = true;
      }
    }
  }

  private static FileFormat convertProtocolFormatToFileFormat(
      MicroBatchPayloadV1.Format protocolFormat) {
    switch (protocolFormat) {
      case AVRO:
        return FileFormat.AVRO;
      case PARQUET:
        return FileFormat.PARQUET;
      case JSON:
        return FileFormat.JSON;
      default:
        throw new IllegalArgumentException("Unsupported format: " + protocolFormat);
    }
  }
}
