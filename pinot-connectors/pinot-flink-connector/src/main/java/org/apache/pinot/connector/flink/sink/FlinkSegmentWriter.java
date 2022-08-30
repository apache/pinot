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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.commons.io.FileUtils;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.pinot.plugin.segmentwriter.filebased.FileBasedSegmentWriter;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.ingestion.segment.writer.SegmentWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link SegmentWriter} implementation that uses a buffer. The {@link GenericRow} are written to
 * the buffer as AVRO records.
 */
@SuppressWarnings("NullAway")
@NotThreadSafe
public class FlinkSegmentWriter extends FileBasedSegmentWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(FlinkSegmentWriter.class);

  private final int _indexOfSubtask;
  private int _rowCount;
  /** A sequence ID that increments each time a segment is flushed */
  private int _seqId;

  // metrics
  private transient Counter _processedRecords;
  private transient volatile long _lastRecordProcessingTimeMs = 0;

  public FlinkSegmentWriter(int indexOfSubtask, MetricGroup metricGroup) {
    _indexOfSubtask = indexOfSubtask;
    registerMetrics(metricGroup);
  }

  @Override
  public void init(TableConfig tableConfig, Schema schema)
      throws Exception {
    _rowCount = 0;
    _seqId = 0;

    Map<String, String> batchConfigMap =
        tableConfig.getIngestionConfig().getBatchIngestionConfig().getBatchConfigMaps().get(0);
    String segmentNamePostfixProp = String.format("%s.%s", BatchConfigProperties.SEGMENT_NAME_GENERATOR_PROP_PREFIX,
        BatchConfigProperties.SEGMENT_NAME_POSTFIX);
    String segmentSuffix = batchConfigMap.get(segmentNamePostfixProp);
    segmentSuffix = segmentSuffix == null ? String.valueOf(_indexOfSubtask) : segmentSuffix + "_" + _indexOfSubtask;
    batchConfigMap.put(segmentNamePostfixProp, segmentSuffix);
    init(tableConfig, schema, batchConfigMap);
  }

  @Override
  public File setStagingDir(TableConfig tableConfig) {
    return new File(FileUtils.getTempDirectory(),
        String.format("segment_writer_staging_%s_%d_%d", tableConfig.getTableName(), _indexOfSubtask,
            System.currentTimeMillis()));
  }

  @Override
  public File getSegmentTarFile(String outputDir, String segmentName) {
    return new File(outputDir, String.format("%s_%d%s", segmentName, _indexOfSubtask, Constants.TAR_GZ_FILE_EXT));
  }

  private void registerMetrics(MetricGroup metricGrp) {
    _processedRecords = metricGrp.counter("records.processed");
    metricGrp.gauge("record.processing.time.ts", (Gauge<Long>) () -> _lastRecordProcessingTimeMs);
  }

  @Override
  public void collect(GenericRow row)
      throws IOException {
    long startTime = System.currentTimeMillis();
    super.collect(row);
    _rowCount++;
    _lastRecordProcessingTimeMs = System.currentTimeMillis() - startTime;
    _processedRecords.inc();
  }

  /**
   * Creates one Pinot segment using the file-based segment writer.
   *
   * <p>Successful completion of segment will return the segment URI, and the URI includes a
   * sequence id indicating the part number. The sequence id is initialized to 0 and each successful
   * flush will increment the sequence id by 1. The segment name will be in the format of
   * tableName_indexOfSubTask_sequenceId (e.g. starbucksStores_1_0).
   *
   * @return URI of the generated segment
   * @throws IOException
   */
  @Override
  public URI flush()
      throws IOException {
    LOGGER.info("Beginning flush for Pinot table with {} records", _rowCount);
    URI uri = super.flush();
    _rowCount = 0;
    _seqId++;
    return uri;
  }

  @Override
  public Integer getSeqId() {
    return _seqId;
  }

  @Override
  public void close()
      throws IOException {
    super.close();
    _rowCount = 0;
    _seqId = 0;
  }
}
