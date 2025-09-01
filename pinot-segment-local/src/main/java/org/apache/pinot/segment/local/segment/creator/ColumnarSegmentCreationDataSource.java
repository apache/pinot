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
package org.apache.pinot.segment.local.segment.creator;

import java.util.Map;
import org.apache.pinot.segment.local.segment.creator.impl.stats.ColumnarSegmentPreIndexStatsContainer;
import org.apache.pinot.segment.spi.creator.SegmentCreationDataSource;
import org.apache.pinot.segment.spi.creator.SegmentPreIndexStatsCollector;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.spi.data.readers.ColumnReader;
import org.apache.pinot.spi.data.readers.ColumnReaderFactory;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * SegmentCreationDataSource implementation that uses ColumnReaderFactory for columnar data access.
 *
 * <p>This data source enables columnar segment building by providing access to column readers
 * instead of row-based record readers. It supports:
 * <ul>
 *   <li>Columnar statistics collection</li>
 *   <li>Column-wise data access through ColumnReaderFactory</li>
 *   <li>Efficient handling of new columns with default values</li>
 *   <li>Data type conversions during schema evolution</li>
 * </ul>
 */
public class ColumnarSegmentCreationDataSource implements SegmentCreationDataSource {
  private static final Logger LOGGER = LoggerFactory.getLogger(ColumnarSegmentCreationDataSource.class);

  private final ColumnReaderFactory _columnReaderFactory;
  private final Map<String, ColumnReader> _columnReaders;

  /**
   * Create a ColumnarSegmentCreationDataSource.
   *
   * @param columnReaderFactory Factory for creating column readers
   * @param columnReaders Map of column name to ColumnReader instances
   */
    public ColumnarSegmentCreationDataSource(ColumnReaderFactory columnReaderFactory,
                                          Map<String, ColumnReader> columnReaders) {
      _columnReaderFactory = columnReaderFactory;
      _columnReaders = columnReaders;
  }

  @Override
  public SegmentPreIndexStatsCollector gatherStats(StatsCollectorConfig statsCollectorConfig) {
    LOGGER.info("Gathering stats using columnar approach for {} columns", _columnReaders.size());

    // Use columnar stats container that efficiently collects statistics column-wise
    return new ColumnarSegmentPreIndexStatsContainer(_columnReaders, statsCollectorConfig);
  }

  @Override
  public RecordReader getRecordReader() {
    // For columnar building, we don't use RecordReader
    // This method is required by the interface but should not be called in columnar mode
    throw new UnsupportedOperationException(
        "RecordReader not supported in columnar mode. Use getColumnReaders() instead.");
  }

  /**
   * Get the column reader factory.
   *
   * @return ColumnReaderFactory instance
   */
  public ColumnReaderFactory getColumnReaderFactory() {
    return _columnReaderFactory;
  }

  /**
   * Get all column readers.
   *
   * @return Map of column name to ColumnReader
   */
  public Map<String, ColumnReader> getColumnReaders() {
    return _columnReaders;
  }
}
