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
package org.apache.pinot.spi.ingestion.segment.writer;

import java.io.Closeable;
import java.net.URI;
import java.util.Map;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * An interface to collect records and create a Pinot segment.
 * This interface helps abstract out details regarding segment generation from the caller.
 */
public interface SegmentWriter extends Closeable {

  /**
   * @see #init(TableConfig, Schema, Map)
   */
  void init(TableConfig tableConfig, Schema schema)
      throws Exception;

  /**
   * Initializes the {@link SegmentWriter} with provided tableConfig and Pinot schema.
   * @param tableConfig The table config for the segment
   * @param schema The Pinot schema for the table
   * @param batchConfigOverride The config override on top of tableConfig
   */
  void init(TableConfig tableConfig, Schema schema, Map<String, String> batchConfigOverride)
      throws Exception;

  /**
   * Collects a single {@link GenericRow} into a buffer.
   * This row is not available in the segment until a <code>flush()</code> is invoked.
   */
  void collect(GenericRow row)
      throws Exception;

  /**
   * Collects a batch of {@link GenericRow}s into a buffer.
   * These rows are not available in the segment until a <code>flush()</code> is invoked.
   */
  default void collect(GenericRow[] rowBatch)
      throws Exception {
    for (GenericRow row : rowBatch) {
      collect(row);
    }
  }

  /**
   * Creates one Pinot segment using the {@link GenericRow}s collected in the buffer,
   * at the outputDirUri as specified in the tableConfig->batchConfigs.
   * Successful invocation of this method means that the {@link GenericRow}s collected so far,
   * are now available in the Pinot segment and not available in the buffer anymore.
   *
   * @return URI of the generated segment
   */
  URI flush()
      throws Exception;
}
