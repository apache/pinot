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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.core.util.SegmentProcessorAvroUtils;
import org.apache.pinot.segment.local.recordtransformer.CompositeTransformer;
import org.apache.pinot.segment.local.recordtransformer.RecordTransformer;
import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.ingestion.batch.BatchConfig;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.ingestion.segment.writer.SegmentWriter;
import org.apache.pinot.spi.recordenricher.RecordEnricherPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link SegmentWriter} implementation that uses a buffer. The {@link GenericRow} are written to
 * the buffer as AVRO records.
 */
@SuppressWarnings("NullAway")
@NotThreadSafe
public class FlinkSegmentWriter implements SegmentWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(FlinkSegmentWriter.class);
  private static final FileFormat BUFFER_FILE_FORMAT = FileFormat.AVRO;

  private final int _indexOfSubtask;

  private TableConfig _tableConfig;
  private String _tableNameWithType;
  private BatchIngestionConfig _batchIngestionConfig;
  private BatchConfig _batchConfig;
  private String _outputDirURI;
  private Schema _schema;
  private Set<String> _fieldsToRead;
  private RecordEnricherPipeline _recordEnricherPipeline;
  private RecordTransformer _recordTransformer;

  private File _stagingDir;
  private File _bufferFile;
  private int _rowCount;
  /** A sequence ID that increments each time a segment is flushed */
  private int _seqId;

  private org.apache.avro.Schema _avroSchema;
  private DataFileWriter<GenericData.Record> _recordWriter;
  private GenericData.Record _reusableRecord;

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
    init(tableConfig, schema, null);
  }

  @Override
  public void init(TableConfig tableConfig, Schema schema, Map<String, String> batchConfigOverride)
      throws Exception {
    _rowCount = 0;
    _seqId = 0;
    _tableConfig = tableConfig;
    _tableNameWithType = _tableConfig.getTableName();
    Preconditions.checkState(
        _tableConfig.getIngestionConfig() != null && _tableConfig.getIngestionConfig().getBatchIngestionConfig() != null
            && CollectionUtils.isNotEmpty(
            _tableConfig.getIngestionConfig().getBatchIngestionConfig().getBatchConfigMaps()),
        "Must provide ingestionConfig->batchIngestionConfig->batchConfigMaps in tableConfig for table: %s",
        _tableNameWithType);
    _batchIngestionConfig = _tableConfig.getIngestionConfig().getBatchIngestionConfig();
    Preconditions.checkState(_batchIngestionConfig.getBatchConfigMaps().size() == 1,
        "batchConfigMaps must contain only 1 BatchConfig for table: %s", _tableNameWithType);

    Map<String, String> batchConfigMap = _batchIngestionConfig.getBatchConfigMaps().get(0);
    String segmentNamePostfixProp = String.format("%s.%s", BatchConfigProperties.SEGMENT_NAME_GENERATOR_PROP_PREFIX,
        BatchConfigProperties.SEGMENT_NAME_POSTFIX);
    String segmentSuffix = batchConfigMap.get(segmentNamePostfixProp);
    segmentSuffix = segmentSuffix == null ? String.valueOf(_indexOfSubtask) : segmentSuffix + "_" + _indexOfSubtask;
    batchConfigMap.put(segmentNamePostfixProp, segmentSuffix);

    _batchConfig = new BatchConfig(_tableNameWithType, batchConfigMap);

    Preconditions.checkState(StringUtils.isNotBlank(_batchConfig.getOutputDirURI()),
        "Must provide: %s in batchConfigs for table: %s", BatchConfigProperties.OUTPUT_DIR_URI, _tableNameWithType);
    _outputDirURI = _batchConfig.getOutputDirURI();
    Files.createDirectories(Paths.get(_outputDirURI));

    _schema = schema;
    _fieldsToRead = _schema.getColumnNames();
    _recordEnricherPipeline = RecordEnricherPipeline.fromTableConfig(_tableConfig);
    _recordTransformer = CompositeTransformer.getDefaultTransformer(_tableConfig, _schema);
    _avroSchema = SegmentProcessorAvroUtils.convertPinotSchemaToAvroSchema(_schema);
    _reusableRecord = new GenericData.Record(_avroSchema);

    // Create tmp dir
    _stagingDir = new File(FileUtils.getTempDirectory(),
        String.format("segment_writer_staging_%s_%d_%d", _tableNameWithType, _indexOfSubtask,
            System.currentTimeMillis()));
    Preconditions.checkState(_stagingDir.mkdirs(), "Failed to create staging dir: %s", _stagingDir.getAbsolutePath());

    // Create buffer file
    File bufferDir = new File(_stagingDir, "buffer_dir");
    Preconditions.checkState(bufferDir.mkdirs(), "Failed to create buffer_dir: %s", bufferDir.getAbsolutePath());
    _bufferFile = new File(bufferDir, "buffer_file");
    resetBuffer();
    LOGGER.info("Initialized {} for Pinot table: {}", this.getClass().getSimpleName(), _tableNameWithType);
  }

  private void registerMetrics(MetricGroup metricGrp) {
    _processedRecords = metricGrp.counter("records.processed");
    metricGrp.gauge("record.processing.time.ts", (Gauge<Long>) () -> _lastRecordProcessingTimeMs);
  }

  private void resetBuffer()
      throws IOException {
    FileUtils.deleteQuietly(_bufferFile);
    _rowCount = 0;
    _recordWriter = new DataFileWriter<>(new GenericDatumWriter<>(_avroSchema));
    _recordWriter.create(_avroSchema, _bufferFile);
  }

  @Override
  public void collect(GenericRow row)
      throws IOException {
    long startTime = System.currentTimeMillis();
    _recordEnricherPipeline.run(row);
    GenericRow transform = _recordTransformer.transform(row);
    SegmentProcessorAvroUtils.convertGenericRowToAvroRecord(transform, _reusableRecord, _fieldsToRead);
    _rowCount++;
    _recordWriter.append(_reusableRecord);
    _lastRecordProcessingTimeMs = System.currentTimeMillis() - startTime;
    _processedRecords.inc();
  }

  /**
   * Creates one Pinot segment using the {@link GenericRow}s collected in the AVRO file buffer, at
   * the outputDirUri as specified in the tableConfig->batchConfigs. Successful invocation of this
   * method means that the {@link GenericRow}s collected so far, are now available in the Pinot
   * segment and not available in the buffer anymore.
   *
   * <p>Successful completion of segment will return the segment URI, and the URI includes a
   * sequence id indicating the part number. The sequence id is initialized to 0 and each successful
   * flush will increment the sequence id by 1. The segment name will be in the format of
   * tableName_indexOfSubTask_sequenceId (e.g. starbucksStores_1_0). The buffer will be reset and
   * ready to accept further records via <code>collect()</code> If an exception is thrown, the buffer
   * will not be reset and so, <code>flush()</code> can be invoked repeatedly in a retry loop. If a
   * successful invocation is not achieved,<code>close()</code> followed by <code>init </code> will
   * have to be called in order to reset the buffer and resume record writing.
   *
   * @return URI of the generated segment
   * @throws IOException
   */
  @Override
  public URI flush()
      throws IOException {

    LOGGER.info("Beginning flush for Pinot table: {} with {} records", _tableNameWithType, _rowCount);
    _recordWriter.close();

    // Create temp dir for flush
    File flushDir = new File(_stagingDir, "flush_dir_" + System.currentTimeMillis());
    Preconditions.checkState(flushDir.mkdirs(), "Failed to create flush dir: %s", flushDir);

    try {
      // Segment dir
      File segmentDir = new File(flushDir, "segment_dir");

      // Make BatchIngestionConfig for flush
      Map<String, String> batchConfigMapOverride = new HashMap<>(_batchConfig.getBatchConfigMap());
      batchConfigMapOverride.put(BatchConfigProperties.INPUT_DIR_URI, _bufferFile.getAbsolutePath());
      batchConfigMapOverride.put(BatchConfigProperties.OUTPUT_DIR_URI, segmentDir.getAbsolutePath());
      batchConfigMapOverride.put(BatchConfigProperties.INPUT_FORMAT, BUFFER_FILE_FORMAT.toString());
      BatchIngestionConfig batchIngestionConfig = new BatchIngestionConfig(Lists.newArrayList(batchConfigMapOverride),
          _batchIngestionConfig.getSegmentIngestionType(), _batchIngestionConfig.getSegmentIngestionFrequency(),
          _batchIngestionConfig.getConsistentDataPush());

      // Build segment
      SegmentGeneratorConfig segmentGeneratorConfig =
          IngestionUtils.generateSegmentGeneratorConfig(_tableConfig, _schema, batchIngestionConfig);
      segmentGeneratorConfig.setSequenceId(_seqId);
      String segmentName = IngestionUtils.buildSegment(segmentGeneratorConfig);
      LOGGER.info("Successfully built segment: {} of sequence {} for Pinot table: {}", segmentName, _seqId,
          _tableNameWithType);

      // Tar segment
      File segmentTarFile =
          new File(_outputDirURI, String.format("%s_%d%s", segmentName, _indexOfSubtask, Constants.TAR_GZ_FILE_EXT));
      if (!_batchConfig.isOverwriteOutput() && segmentTarFile.exists()) {
        segmentTarFile = new File(_outputDirURI,
            String.format("%s_%d%s", segmentName, System.currentTimeMillis(), Constants.TAR_GZ_FILE_EXT));
      }
      TarGzCompressionUtils.createTarGzFile(new File(segmentDir, segmentName), segmentTarFile);
      LOGGER.info("Created segment tar: {} for segment: {} of Pinot table: {}", segmentTarFile.getAbsolutePath(),
          segmentName, _tableNameWithType);

      // Reset buffer and return segmentTar URI
      resetBuffer();
      // increment the sequence id
      _seqId++;
      return segmentTarFile.toURI();
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while generating segment from buffer file: %s for table:%s",
              _bufferFile.getAbsolutePath(), _tableNameWithType), e);
    } finally {
      FileUtils.deleteQuietly(flushDir);
    }
  }

  @Override
  public void close()
      throws IOException {
    LOGGER.info("Closing {} for Pinot table: {}", this.getClass().getSimpleName(), _tableNameWithType);
    _recordWriter.close();
    resetBuffer();
    _seqId = 0;
    FileUtils.deleteQuietly(_stagingDir);
  }
}
