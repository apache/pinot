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
package org.apache.pinot.connector.flink.sink;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.pinot.connector.flink.common.PinotGenericRowConverter;
import org.apache.pinot.plugin.segmentuploader.SegmentUploaderDefault;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.ingestion.segment.uploader.SegmentUploader;
import org.apache.pinot.spi.ingestion.segment.writer.SegmentWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A Flink 2.x {@link Sink} that converts stream records into Pinot segments and uploads them to Pinot.
 *
 * <p>The sink is stateless and provides at-least-once semantics by draining in-flight uploads whenever Flink asks the
 * writer to flush.
 *
 * @param <T> type of record supported by the sink
 */
@SuppressWarnings("NullAway")
public class PinotSink<T> implements Sink<T> {
  public static final long DEFAULT_SEGMENT_FLUSH_MAX_NUM_RECORDS = 500000;
  public static final int DEFAULT_EXECUTOR_POOL_SIZE = 5;
  public static final long DEFAULT_EXECUTOR_SHUTDOWN_WAIT_MS = 3000;
  public static final String DEFAULT_OUTPUT_DIR_URI = "/tmp/pinotoutput";

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(PinotSink.class);

  private final long _segmentFlushMaxNumRecords;
  private final int _executorPoolSize;
  private final PinotGenericRowConverter<T> _recordConverter;
  private final TableConfig _tableConfig;
  private final Schema _schema;
  @Nullable private final String _segmentNamePrefix;
  @Nullable private final Long _segmentUploadTimeMs;

  public PinotSink(PinotGenericRowConverter<T> recordConverter, TableConfig tableConfig, Schema schema) {
    this(recordConverter, tableConfig, schema, DEFAULT_SEGMENT_FLUSH_MAX_NUM_RECORDS, DEFAULT_EXECUTOR_POOL_SIZE);
  }

  /**
   * Creates a sink using a table config fetched from the controller and injects the upload settings required by the
   * Flink connector.
   */
  public PinotSink(PinotGenericRowConverter<T> recordConverter, TableConfig tableConfig, Schema schema,
      String controllerBaseUrl) {
    this(recordConverter, prepareTableConfigForSink(tableConfig, controllerBaseUrl), schema,
        DEFAULT_SEGMENT_FLUSH_MAX_NUM_RECORDS, DEFAULT_EXECUTOR_POOL_SIZE);
  }

  public PinotSink(PinotGenericRowConverter<T> recordConverter, TableConfig tableConfig, Schema schema,
      long segmentFlushMaxNumRecords, int executorPoolSize) {
    this(recordConverter, tableConfig, schema, segmentFlushMaxNumRecords, executorPoolSize, null, null);
  }

  public PinotSink(PinotGenericRowConverter<T> recordConverter, TableConfig tableConfig, Schema schema,
      long segmentFlushMaxNumRecords, int executorPoolSize, @Nullable String segmentNamePrefix,
      @Nullable Long segmentUploadTimeMs) {
    _recordConverter = recordConverter;
    _tableConfig = tableConfig;
    _schema = schema;
    _segmentFlushMaxNumRecords = segmentFlushMaxNumRecords;
    _executorPoolSize = executorPoolSize;
    _segmentNamePrefix = segmentNamePrefix;
    _segmentUploadTimeMs = segmentUploadTimeMs;
  }

  /**
   * Applies the connector-specific batch-ingestion defaults needed for segment generation and upload.
   */
  static TableConfig prepareTableConfigForSink(TableConfig tableConfig, String controllerBaseUrl) {
    Map<String, String> requiredBatchConfig = new HashMap<>();
    requiredBatchConfig.put(BatchConfigProperties.PUSH_CONTROLLER_URI, controllerBaseUrl);
    requiredBatchConfig.put(BatchConfigProperties.OUTPUT_DIR_URI, DEFAULT_OUTPUT_DIR_URI);

    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    if (ingestionConfig == null) {
      ingestionConfig = new IngestionConfig();
      ingestionConfig.setBatchIngestionConfig(
          new BatchIngestionConfig(Collections.singletonList(requiredBatchConfig), "APPEND", "HOURLY"));
      tableConfig.setIngestionConfig(ingestionConfig);
      return tableConfig;
    }

    BatchIngestionConfig batchIngestionConfig = ingestionConfig.getBatchIngestionConfig();
    if (batchIngestionConfig == null) {
      ingestionConfig.setBatchIngestionConfig(
          new BatchIngestionConfig(Collections.singletonList(requiredBatchConfig), "APPEND", "HOURLY"));
      return tableConfig;
    }

    List<Map<String, String>> batchConfigMaps = batchIngestionConfig.getBatchConfigMaps();
    if (batchConfigMaps == null || batchConfigMaps.isEmpty()) {
      batchIngestionConfig.setBatchConfigMaps(Collections.singletonList(requiredBatchConfig));
    } else if (batchConfigMaps.size() > 1) {
      throw new IllegalStateException(String.format(
          "Flink connector requires exactly 1 batchConfigMap for table %s, got %d", tableConfig.getTableName(),
          batchConfigMaps.size()));
    } else {
      Map<String, String> mergedBatchConfig = new HashMap<>(batchConfigMaps.get(0));
      mergedBatchConfig.putAll(requiredBatchConfig);
      batchIngestionConfig.setBatchConfigMaps(Collections.singletonList(mergedBatchConfig));
    }
    return tableConfig;
  }

  @Override
  public SinkWriter<T> createWriter(WriterInitContext context)
      throws IOException {
    return new PinotSinkWriter(context);
  }

  /**
   * Writer implementation used by the Flink runtime inside each sink subtask.
   *
   * <p>This writer is not thread-safe; Flink invokes it from a single task thread while uploads are dispatched to a
   * bounded background pool.
   */
  private final class PinotSinkWriter implements SinkWriter<T> {
    private final SegmentWriter _segmentWriter;
    private final SegmentUploader _segmentUploader;
    private final ExecutorService _executor;
    private final List<Future<?>> _pendingUploads = new ArrayList<>();
    private long _segmentNumRecord;

    private PinotSinkWriter(WriterInitContext context)
        throws IOException {
      _executor = Executors.newFixedThreadPool(_executorPoolSize);

      SegmentWriter segmentWriter = null;
      SegmentUploader segmentUploader = null;
      try {
        int subtaskIndex = context.getTaskInfo().getIndexOfThisSubtask();
        segmentWriter =
            new FlinkSegmentWriter(subtaskIndex, context.metricGroup(), _segmentNamePrefix, _segmentUploadTimeMs);
        segmentWriter.init(_tableConfig, _schema);
        segmentUploader = new SegmentUploaderDefault();
        segmentUploader.init(_tableConfig);
        _segmentWriter = segmentWriter;
        _segmentUploader = segmentUploader;
        LOG.info("Open Pinot sink with the table {}", _tableConfig.toJsonString());
      } catch (Exception e) {
        shutdownExecutorNow();
        closeQuietly(segmentWriter);
        throw new IOException("Failed to initialize Pinot sink writer", e);
      }
    }

    @Override
    public void write(T element, Context context)
        throws IOException, InterruptedException {
      checkCompletedUploads();
      try {
        _segmentWriter.collect(_recordConverter.convertToRow(element));
      } catch (Exception e) {
        throw new IOException("Failed to write record to Pinot segment buffer", e);
      }
      _segmentNumRecord++;
      if (_segmentNumRecord >= _segmentFlushMaxNumRecords) {
        flushBufferAsync();
      }
    }

    @Override
    public void flush(boolean endOfInput)
        throws IOException, InterruptedException {
      if (_segmentNumRecord > 0) {
        flushBufferAsync();
      }
      waitForPendingUploads();
    }

    @Override
    public void close()
        throws Exception {
      Exception failure = null;
      LOG.info("Closing Pinot sink");
      try {
        flush(true);
      } catch (Exception e) {
        restoreInterruptFlag(e);
        failure = e;
      } finally {
        try {
          shutdownExecutor();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          if (failure == null) {
            failure = e;
          } else {
            failure.addSuppressed(e);
          }
        }
        try {
          _segmentWriter.close();
        } catch (Exception e) {
          if (failure == null) {
            failure = e;
          } else {
            failure.addSuppressed(e);
          }
        }
      }

      if (failure != null) {
        throw failure;
      }
    }

    private void flushBufferAsync()
        throws IOException {
      final URI segmentURI;
      try {
        segmentURI = _segmentWriter.flush();
      } catch (Exception e) {
        throw new IOException("Failed to flush Pinot segment buffer", e);
      }

      long recordsInSegment = _segmentNumRecord;
      _segmentNumRecord = 0;
      LOG.info("Pinot segment writer flushed with {} records to {}", recordsInSegment, segmentURI);
      _pendingUploads.add(_executor.submit(() -> {
        try {
          _segmentUploader.uploadSegment(segmentURI, null);
        } catch (Exception e) {
          throw new RuntimeException("Failed to upload Pinot segment", e);
        }
        LOG.info("Pinot segment uploaded to {}", segmentURI);
      }));
    }

    private void checkCompletedUploads()
        throws IOException, InterruptedException {
      Iterator<Future<?>> iterator = _pendingUploads.iterator();
      while (iterator.hasNext()) {
        Future<?> pendingUpload = iterator.next();
        if (pendingUpload.isDone()) {
          awaitUpload(pendingUpload);
          iterator.remove();
        }
      }
    }

    private void waitForPendingUploads()
        throws IOException, InterruptedException {
      for (Future<?> pendingUpload : _pendingUploads) {
        awaitUpload(pendingUpload);
      }
      _pendingUploads.clear();
    }

    private void awaitUpload(Future<?> pendingUpload)
        throws IOException, InterruptedException {
      try {
        pendingUpload.get();
      } catch (ExecutionException e) {
        Throwable cause = unwrapExecutionFailure(e.getCause());
        if (cause instanceof IOException) {
          throw (IOException) cause;
        }
        if (cause instanceof InterruptedException) {
          Thread.currentThread().interrupt();
          throw (InterruptedException) cause;
        }
        throw new IOException("Failed to upload Pinot segment", cause);
      }
    }

    private Throwable unwrapExecutionFailure(Throwable throwable) {
      if (throwable instanceof RuntimeException && throwable.getCause() != null) {
        return throwable.getCause();
      }
      return throwable;
    }

    private void shutdownExecutor()
        throws InterruptedException {
      _executor.shutdown();
      if (!_executor.awaitTermination(DEFAULT_EXECUTOR_SHUTDOWN_WAIT_MS, TimeUnit.MILLISECONDS)) {
        _executor.shutdownNow();
      }
    }

    private void shutdownExecutorNow() {
      _executor.shutdownNow();
    }

    private void restoreInterruptFlag(Exception exception) {
      if (exception instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
    }

    private void closeQuietly(@Nullable SegmentWriter closeable) {
      if (closeable != null) {
        try {
          closeable.close();
        } catch (Exception e) {
          LOG.warn("Failed to close Pinot segment writer during cleanup", e);
        }
      }
    }
  }
}
