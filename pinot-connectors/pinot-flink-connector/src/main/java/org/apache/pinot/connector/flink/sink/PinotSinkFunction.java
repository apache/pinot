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

import javax.annotation.Nullable;
import org.apache.pinot.connector.flink.common.PinotGenericRowConverter;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


/**
 * Backward-compatible shim for code that still constructs the old sink class name.
 *
 * <p>Flink 2.x removed the legacy {@code SinkFunction} API. Use {@link PinotSink} together with
 * {@code DataStream#sinkTo(Sink)} for new code.
 *
 * @param <T> type of record supported by the sink
 */
@Deprecated
public class PinotSinkFunction<T> extends PinotSink<T> {
  private static final long serialVersionUID = 1L;
  @Deprecated
  public static final String DEFAULT_OUTPUT_DIR_URI = PinotSink.DEFAULT_OUTPUT_DIR_URI;
  @Deprecated
  public static final long DEFAULT_SEGMENT_FLUSH_MAX_NUM_RECORDS = PinotSink.DEFAULT_SEGMENT_FLUSH_MAX_NUM_RECORDS;
  @Deprecated
  public static final int DEFAULT_EXECUTOR_POOL_SIZE = PinotSink.DEFAULT_EXECUTOR_POOL_SIZE;

  public PinotSinkFunction(PinotGenericRowConverter<T> recordConverter, TableConfig tableConfig, Schema schema) {
    super(recordConverter, tableConfig, schema);
  }

  public PinotSinkFunction(PinotGenericRowConverter<T> recordConverter, TableConfig tableConfig, Schema schema,
      String controllerBaseUrl) {
    super(recordConverter, tableConfig, schema, controllerBaseUrl);
  }

  public PinotSinkFunction(PinotGenericRowConverter<T> recordConverter, TableConfig tableConfig, Schema schema,
      long segmentFlushMaxNumRecords, int executorPoolSize) {
    super(recordConverter, tableConfig, schema, segmentFlushMaxNumRecords, executorPoolSize);
  }

  public PinotSinkFunction(PinotGenericRowConverter<T> recordConverter, TableConfig tableConfig, Schema schema,
      long segmentFlushMaxNumRecords, int executorPoolSize, @Nullable String segmentNamePrefix,
      @Nullable Long segmentUploadTimeMs) {
    super(recordConverter, tableConfig, schema, segmentFlushMaxNumRecords, executorPoolSize, segmentNamePrefix,
        segmentUploadTimeMs);
  }

  static TableConfig prepareTableConfigForSink(TableConfig tableConfig, String controllerBaseUrl) {
    return PinotSink.prepareTableConfigForSink(tableConfig, controllerBaseUrl);
  }
}
