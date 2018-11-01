/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.creator;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.data.recordtransformer.CompoundTransformer;
import com.linkedin.pinot.core.data.recordtransformer.RecordTransformer;
import com.linkedin.pinot.core.segment.creator.impl.stats.SegmentPreIndexStatsCollectorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link com.linkedin.pinot.core.segment.creator.SegmentCreationDataSource} that uses a
 * {@link com.linkedin.pinot.core.data.readers.RecordReader} as the underlying data source.
 */
// TODO: make it Closeable so that resource in record reader can be released
public class RecordReaderSegmentCreationDataSource implements SegmentCreationDataSource {
  private static final Logger LOGGER = LoggerFactory.getLogger(RecordReaderSegmentCreationDataSource.class);

  private final RecordReader _recordReader;

  public RecordReaderSegmentCreationDataSource(RecordReader recordReader) {
    _recordReader = recordReader;
  }

  @Override
  public SegmentPreIndexStatsCollector gatherStats(StatsCollectorConfig statsCollectorConfig) {
    try {
      RecordTransformer recordTransformer =
          CompoundTransformer.getDefaultTransformer(statsCollectorConfig.getSchema());

      SegmentPreIndexStatsCollector collector = new SegmentPreIndexStatsCollectorImpl(statsCollectorConfig);
      collector.init();

      // Gather the stats
      GenericRow readRow = null;
      while (_recordReader.hasNext()) {
        readRow = GenericRow.createOrReuseRow(readRow);
        GenericRow transformedRow = recordTransformer.transform(_recordReader.next(readRow));
        if (transformedRow != null) {
          collector.collectRow(transformedRow);
        }
      }

      collector.build();
      return collector;
    } catch (Exception e) {
      LOGGER.error("Caught exception while gathering stats", e);
      Utils.rethrowException(e);
      return null;
    }
  }

  @Override
  public RecordReader getRecordReader() {
    try {
      _recordReader.rewind();
    } catch (Exception e) {
      LOGGER.error("Caught exception while rewinding record reader", e);
      Utils.rethrowException(e);
    }

    return _recordReader;
  }
}
