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

import org.apache.pinot.common.Utils;
import org.apache.pinot.segment.local.recordenricher.RecordEnricherPipeline;
import org.apache.pinot.segment.local.segment.creator.impl.stats.SegmentPreIndexStatsCollectorImpl;
import org.apache.pinot.segment.spi.creator.SegmentCreationDataSource;
import org.apache.pinot.segment.spi.creator.SegmentPreIndexStatsCollector;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link SegmentCreationDataSource} that uses a
 * {@link RecordReader} as the underlying data source.
 */
// TODO: make it Closeable so that resource in record reader can be released
public class RecordReaderSegmentCreationDataSource implements SegmentCreationDataSource {
  private static final Logger LOGGER = LoggerFactory.getLogger(RecordReaderSegmentCreationDataSource.class);

  private final RecordReader _recordReader;
  private RecordEnricherPipeline _recordEnricherPipeline;
  private TransformPipeline _transformPipeline;

  public RecordReaderSegmentCreationDataSource(RecordReader recordReader) {
    _recordReader = recordReader;
  }

  public void setRecordEnricherPipeline(RecordEnricherPipeline recordEnricherPipeline) {
    _recordEnricherPipeline = recordEnricherPipeline;
  }

  public void setTransformPipeline(TransformPipeline transformPipeline) {
    _transformPipeline = transformPipeline;
  }

  @Override
  public SegmentPreIndexStatsCollector gatherStats(StatsCollectorConfig statsCollectorConfig) {
    try {
      RecordEnricherPipeline recordEnricherPipeline = _recordEnricherPipeline != null ? _recordEnricherPipeline
          : RecordEnricherPipeline.fromTableConfig(statsCollectorConfig.getTableConfig());
      TransformPipeline transformPipeline = _transformPipeline != null ? _transformPipeline
          : new TransformPipeline(statsCollectorConfig.getTableConfig(), statsCollectorConfig.getSchema());

      SegmentPreIndexStatsCollector collector = new SegmentPreIndexStatsCollectorImpl(statsCollectorConfig);
      collector.init();

      // Gather the stats
      GenericRow reuse = new GenericRow();
      TransformPipeline.Result reusedResult = new TransformPipeline.Result();
      while (_recordReader.hasNext()) {
        reuse.clear();

        reuse = _recordReader.next(reuse);
        recordEnricherPipeline.run(reuse);
        transformPipeline.processRow(reuse, reusedResult);
        for (GenericRow row : reusedResult.getTransformedRows()) {
          collector.collectRow(row);
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
