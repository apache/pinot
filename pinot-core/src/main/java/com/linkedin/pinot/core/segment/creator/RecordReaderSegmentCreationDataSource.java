/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.core.data.extractors.FieldExtractor;
import com.linkedin.pinot.core.data.extractors.FieldExtractorFactory;
import com.linkedin.pinot.core.data.extractors.PlainFieldExtractor;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.segment.creator.impl.stats.SegmentPreIndexStatsCollectorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link com.linkedin.pinot.core.segment.creator.SegmentCreationDataSource} that uses a
 * {@link com.linkedin.pinot.core.data.readers.RecordReader} as the underlying data source.
 */
public class RecordReaderSegmentCreationDataSource implements SegmentCreationDataSource {
  private static final Logger LOGGER = LoggerFactory.getLogger(RecordReaderSegmentCreationDataSource.class);

  public RecordReaderSegmentCreationDataSource(RecordReader recordReader) {
    _recordReader = recordReader;

    try {
      recordReader.init();
    } catch (Exception e) {
      LOGGER.error("Caught exception while initializing record reader", e);
      Utils.rethrowException(e);
    }

  }

  private RecordReader _recordReader;

  @Override
  public SegmentPreIndexStatsCollector gatherStats(StatsCollectorConfig statsCollectorConfig) {
    try {
      PlainFieldExtractor fieldExtractor = FieldExtractorFactory.getPlainFieldExtractor(statsCollectorConfig.getSchema());

      SegmentPreIndexStatsCollector collector = new SegmentPreIndexStatsCollectorImpl(statsCollectorConfig);
      collector.init();

      // Gather the stats
      GenericRow readRow = new GenericRow();
      GenericRow transformedRow = new GenericRow();
      while (_recordReader.hasNext()) {
        transformedRow = readNextRowSanitized(readRow, transformedRow, fieldExtractor);
        collector.collectRow(transformedRow);
      }

      collector.build();
      return collector;
    } catch (Exception e) {
      LOGGER.error("Caught exception while gathering stats", e);
      Utils.rethrowException(e);
      return null;
    }
  }

  private GenericRow readNextRowSanitized(GenericRow readRow, GenericRow transformedRow, FieldExtractor extractor) {
    readRow = GenericRow.createOrReuseRow(readRow);
    readRow = _recordReader.next(readRow);
    transformedRow = GenericRow.createOrReuseRow(transformedRow);
    return extractor.transform(readRow, transformedRow);
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
