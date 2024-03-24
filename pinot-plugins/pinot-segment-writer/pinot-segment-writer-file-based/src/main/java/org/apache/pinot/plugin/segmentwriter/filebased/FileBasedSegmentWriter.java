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
package org.apache.pinot.plugin.segmentwriter.filebased;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link SegmentWriter} implementation that uses a local file as a buffer to collect {@link GenericRow}.
 * The {@link GenericRow} are written to the buffer as AVRO records.
 */
@NotThreadSafe
public class FileBasedSegmentWriter implements SegmentWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedSegmentWriter.class);
  private static final FileFormat BUFFER_FILE_FORMAT = FileFormat.AVRO;

  private TableConfig _tableConfig;
  private String _tableNameWithType;
  private BatchIngestionConfig _batchIngestionConfig;
  private BatchConfig _batchConfig;
  private String _outputDirURI;
  private Schema _schema;
  private Set<String> _fieldsToRead;
  private RecordTransformer _recordTransformer;

  private File _stagingDir;
  private File _bufferFile;

  private org.apache.avro.Schema _avroSchema;
  private DataFileWriter<GenericData.Record> _recordWriter;
  private GenericData.Record _reusableRecord;

  @Override
  public void init(TableConfig tableConfig, Schema schema) throws Exception {
    init(tableConfig, schema, Collections.emptyMap());
  }

  @Override
  public void init(TableConfig tableConfig, Schema schema, Map<String, String> batchConfigOverride)
      throws Exception {
    _tableConfig = tableConfig;
    _tableNameWithType = _tableConfig.getTableName();

    Preconditions.checkState(
        _tableConfig.getIngestionConfig() != null && _tableConfig.getIngestionConfig().getBatchIngestionConfig() != null
            && CollectionUtils
            .isNotEmpty(_tableConfig.getIngestionConfig().getBatchIngestionConfig().getBatchConfigMaps()),
        "Must provide ingestionConfig->batchIngestionConfig->batchConfigMaps in tableConfig for table: %s",
        _tableNameWithType);
    _batchIngestionConfig = _tableConfig.getIngestionConfig().getBatchIngestionConfig();
    Preconditions.checkState(_batchIngestionConfig.getBatchConfigMaps().size() == 1,
        "batchConfigMaps must contain only 1 BatchConfig for table: %s", _tableNameWithType);

    // apply config override provided by user.
    Map<String, String> batchConfigMap = new HashMap<>(_batchIngestionConfig.getBatchConfigMaps().get(0));
    batchConfigMap.putAll(batchConfigOverride);
    _batchConfig = new BatchConfig(_tableNameWithType, batchConfigMap);

    Preconditions.checkState(StringUtils.isNotBlank(_batchConfig.getOutputDirURI()),
        "Must provide: %s in batchConfigs for table: %s", BatchConfigProperties.OUTPUT_DIR_URI, _tableNameWithType);
    _outputDirURI = _batchConfig.getOutputDirURI();
    Files.createDirectories(Paths.get(_outputDirURI));

    _schema = schema;
    _fieldsToRead = _schema.getColumnNames();
    _recordTransformer = CompositeTransformer.getDefaultTransformer(_tableConfig, _schema);
    _avroSchema = SegmentProcessorAvroUtils.convertPinotSchemaToAvroSchema(_schema);
    _reusableRecord = new GenericData.Record(_avroSchema);

    // Create tmp dir
    _stagingDir = new File(FileUtils.getTempDirectory(),
        String.format("segment_writer_staging_%s_%d", _tableNameWithType, System.currentTimeMillis()));
    Preconditions.checkState(_stagingDir.mkdirs(), "Failed to create staging dir: %s", _stagingDir.getAbsolutePath());

    // Create buffer file
    File bufferDir = new File(_stagingDir, "buffer_dir");
    Preconditions.checkState(bufferDir.mkdirs(), "Failed to create buffer_dir: %s", bufferDir.getAbsolutePath());
    _bufferFile = new File(bufferDir, "buffer_file");
    resetBuffer();
    LOGGER.info("Initialized {} for table: {}", FileBasedSegmentWriter.class.getName(), _tableNameWithType);
  }

  private void resetBuffer()
      throws IOException {
    FileUtils.deleteQuietly(_bufferFile);
    _recordWriter = new DataFileWriter<>(new GenericDatumWriter<>(_avroSchema));
    _recordWriter.create(_avroSchema, _bufferFile);
  }

  @Override
  public void collect(GenericRow row)
      throws IOException {
    GenericRow transform = _recordTransformer.transform(row);
    SegmentProcessorAvroUtils.convertGenericRowToAvroRecord(transform, _reusableRecord, _fieldsToRead);
    _recordWriter.append(_reusableRecord);
  }

  /**
   * Creates one Pinot segment using the {@link GenericRow}s collected in the AVRO file buffer,
   * at the outputDirUri as specified in the tableConfig->batchConfigs.
   * Successful invocation of this method means that the {@link GenericRow}s collected so far,
   * are now available in the Pinot segment and not available in the buffer anymore.
   *
   * Successful completion of segment will return the segment URI.
   * The buffer will be reset and ready to accept further records via <code>collect()</code>
   *
   * If an exception is thrown, the buffer will not be reset
   * and so, <code>flush()</code> can be invoked repeatedly in a retry loop.
   * If a successful invocation is not achieved,<code>close()</code> followed by <code>init</code> will have to be
   * called in order to reset the buffer and resume record writing.
   *
   * @return URI of the generated segment
   * @throws IOException
   */
  @Override
  public URI flush()
      throws IOException {

    LOGGER.info("Beginning flush for table: {}", _tableNameWithType);
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
      String segmentName = IngestionUtils.buildSegment(segmentGeneratorConfig);
      LOGGER.info("Successfully built segment: {} for table: {}", segmentName, _tableNameWithType);

      // Tar segment
      File segmentTarFile = new File(_outputDirURI, segmentName + Constants.TAR_GZ_FILE_EXT);
      if (segmentTarFile.exists()) {
        if (!_batchConfig.isOverwriteOutput()) {
          throw new IllegalArgumentException(String.format("Duplicate segment name generated '%s' in '%s', please "
              + "adjust segment name generator config to avoid duplicates, or allow batch config overwrite",
              segmentName, _outputDirURI));
        } else {
          LOGGER.warn(String.format("Duplicate segment name detected '%s' in file '%s', deleting old segment",
              segmentName, segmentDir));
          if (segmentTarFile.delete()) {
            LOGGER.warn(String.format("Segment file deleted: '%s/%s'", _outputDirURI, segmentName));
          }
        }
      }
      TarGzCompressionUtils.createTarGzFile(new File(segmentDir, segmentName), segmentTarFile);
      LOGGER.info("Created segment tar: {} for segment: {} of table: {}", segmentTarFile.getAbsolutePath(), segmentName,
          _tableNameWithType);

      // Reset buffer and return segmentTar URI
      resetBuffer();
      return segmentTarFile.toURI();
    } catch (Exception e) {
      throw new RuntimeException(String
          .format("Caught exception while generating segment from buffer file: %s for table:%s",
              _bufferFile.getAbsolutePath(), _tableNameWithType), e);
    } finally {
      FileUtils.deleteQuietly(flushDir);
    }
  }

  @Override
  public void close()
      throws IOException {
    LOGGER.info("Closing {} for table: {}", FileBasedSegmentWriter.class.getName(), _tableNameWithType);
    _recordWriter.close();
    FileUtils.deleteQuietly(_stagingDir);
  }
}
