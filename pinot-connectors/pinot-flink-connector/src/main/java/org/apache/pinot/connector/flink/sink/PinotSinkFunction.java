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

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.pinot.connector.flink.common.PinotGenericRowConverter;
import org.apache.pinot.plugin.segmentuploader.SegmentUploaderDefault;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.ingestion.segment.uploader.SegmentUploader;
import org.apache.pinot.spi.ingestion.segment.writer.SegmentWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The sink function for Pinot.
 *
 * @param <T> type of record supported
 */
@SuppressWarnings("NullAway")
public class PinotSinkFunction<T> extends RichSinkFunction<T> implements CheckpointedFunction {

  public static final long DEFAULT_SEGMENT_FLUSH_MAX_NUM_RECORDS = 500000;
  public static final int DEFAULT_EXECUTOR_POOL_SIZE = 5;
  public static final long DEFAULT_EXECUTOR_SHUTDOWN_WAIT_MS = 3000;

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(PinotSinkFunction.class);

  private final long _segmentFlushMaxNumRecords;
  private final int _executorPoolSize;

  private final PinotGenericRowConverter<T> _recordConverter;

  private TableConfig _tableConfig;
  private Schema _schema;

  private ListState<GenericRow> _pendingRows;

  private transient SegmentWriter _segmentWriter;
  private transient SegmentUploader _segmentUploader;
  private transient ExecutorService _executor;
  private transient long _segmentNumRecord;
  private transient boolean _isCheckpointingEnabled;

  public PinotSinkFunction(PinotGenericRowConverter<T> recordConverter, TableConfig tableConfig, Schema schema) {
    this(recordConverter, tableConfig, schema, DEFAULT_SEGMENT_FLUSH_MAX_NUM_RECORDS, DEFAULT_EXECUTOR_POOL_SIZE);
  }

  public PinotSinkFunction(PinotGenericRowConverter<T> recordConverter, TableConfig tableConfig, Schema schema,
      long segmentFlushMaxNumRecords, int executorPoolSize) {
    _recordConverter = recordConverter;
    _tableConfig = tableConfig;
    _schema = schema;
    _segmentFlushMaxNumRecords = segmentFlushMaxNumRecords;
    _executorPoolSize = executorPoolSize;
  }

  @Override
  public void open(Configuration parameters)
      throws Exception {
    int indexOfSubtask = this.getRuntimeContext().getIndexOfThisSubtask();
    _segmentWriter = new FlinkSegmentWriter(indexOfSubtask, getRuntimeContext().getMetricGroup());
    _segmentWriter.init(_tableConfig, _schema);
    _segmentUploader = new SegmentUploaderDefault();
    _segmentUploader.init(_tableConfig);
    _segmentNumRecord = 0;
    _executor = Executors.newFixedThreadPool(_executorPoolSize);
    _isCheckpointingEnabled = ((StreamingRuntimeContext) this.getRuntimeContext()).isCheckpointingEnabled();

    LOG.info("Open Pinot Sink with the table {}", _tableConfig.toJsonString());
  }

  @Override
  public void close()
      throws Exception {
    LOG.info("Closing Pinot Sink");
    try {
      if (_segmentNumRecord > 0 && !_isCheckpointingEnabled) {
        flush();
      }
    } catch (Exception e) {
      LOG.error("Error when closing Pinot sink", e);
    }
    _executor.shutdown();
    try {
      if (!_executor.awaitTermination(DEFAULT_EXECUTOR_SHUTDOWN_WAIT_MS, TimeUnit.MILLISECONDS)) {
        _executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      _executor.shutdownNow();
    } finally {
      _segmentWriter.close();
    }
  }

  @Override
  public void invoke(T value, Context context)
      throws Exception {
    GenericRow row = _recordConverter.convertToRow(value);
    if (_isCheckpointingEnabled) {
      _pendingRows.add(row);
    }
    _segmentWriter.collect(row);
    _segmentNumRecord++;
    if (_segmentNumRecord >= _segmentFlushMaxNumRecords) {
      flush();
    }
  }

  private void flush()
      throws Exception {
    URI segmentURI = _segmentWriter.flush();
    LOG.info("Pinot segment writer flushed with {} records to {}", _segmentNumRecord, segmentURI);
    _segmentNumRecord = 0;
    if (_isCheckpointingEnabled) {
      _pendingRows.clear();
    }

    _executor.submit(() -> {
      try {
        _segmentUploader.uploadSegment(segmentURI, null);
      } catch (Exception e) {
        throw new RuntimeException("Failed to upload pinot segment", e);
      }
      LOG.info("Pinot segment uploaded to {}", segmentURI);
    });
  }


  @Override
  public void snapshotState(FunctionSnapshotContext functionSnapshotContext)
      throws Exception {
      // nothing to do here, state is already up to date
  }

  @Override
  public void initializeState(FunctionInitializationContext functionInitializationContext)
      throws Exception {
    _pendingRows = functionInitializationContext.getOperatorStateStore()
        .getListState(new ListStateDescriptor("pending-rows", TypeInformation.of(GenericRow.class)));

    for (GenericRow row: _pendingRows.get()) {
      _segmentWriter.collect(row);
      _segmentNumRecord++;
    }
  }
}
