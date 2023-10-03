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
package org.apache.pinot.segment.local.segment.creator.impl;

import com.google.common.base.Preconditions;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.recordtransformer.ComplexTypeTransformer;
import org.apache.pinot.segment.local.recordtransformer.RecordTransformer;
import org.apache.pinot.segment.local.segment.creator.RecordReaderSegmentCreationDataSource;
import org.apache.pinot.segment.local.segment.creator.TransformPipeline;
import org.apache.pinot.segment.local.segment.index.converter.SegmentFormatConverterFactory;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.local.startree.v2.builder.MultipleTreesBuilder;
import org.apache.pinot.segment.local.utils.CrcUtils;
import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.converter.SegmentFormatConverter;
import org.apache.pinot.segment.spi.creator.ColumnIndexCreationInfo;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.creator.SegmentCreationDataSource;
import org.apache.pinot.segment.spi.creator.SegmentCreator;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.creator.SegmentPreIndexStatsContainer;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.creator.SegmentIndexCreationInfo;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.IngestionSchemaValidator;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.SchemaValidatorFactory;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.ReadMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of an index segment creator.
 */
// TODO: Check resource leaks
public class SegmentIndexCreationDriverImpl implements SegmentIndexCreationDriver {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentIndexCreationDriverImpl.class);

  private SegmentGeneratorConfig _config;
  private RecordReader _recordReader;
  private SegmentPreIndexStatsContainer _segmentStats;
  // NOTE: Use TreeMap so that the columns are ordered alphabetically
  private TreeMap<String, ColumnIndexCreationInfo> _indexCreationInfoMap;
  private SegmentCreator _indexCreator;
  private SegmentIndexCreationInfo _segmentIndexCreationInfo;
  private Schema _dataSchema;
  private TransformPipeline _transformPipeline;
  private IngestionSchemaValidator _ingestionSchemaValidator;
  private int _totalDocs = 0;
  private File _tempIndexDir;
  private String _segmentName;
  private long _totalRecordReadTime = 0;
  private long _totalIndexTime = 0;
  private long _totalStatsCollectorTime = 0;
  private boolean _continueOnError;

  @Override
  public void init(SegmentGeneratorConfig config)
      throws Exception {
    init(config, getRecordReader(config));
  }

  private RecordReader getRecordReader(SegmentGeneratorConfig segmentGeneratorConfig)
      throws Exception {
    File dataFile = new File(segmentGeneratorConfig.getInputFilePath());
    Preconditions.checkState(dataFile.exists(), "Input file: " + dataFile.getAbsolutePath() + " does not exist");

    Schema schema = segmentGeneratorConfig.getSchema();
    TableConfig tableConfig = segmentGeneratorConfig.getTableConfig();
    FileFormat fileFormat = segmentGeneratorConfig.getFormat();
    String recordReaderClassName = segmentGeneratorConfig.getRecordReaderPath();
    Set<String> sourceFields = IngestionUtils
        .getFieldsForRecordExtractor(tableConfig.getIngestionConfig(), segmentGeneratorConfig.getSchema());

    // Allow for instantiation general record readers from a record reader path passed into segment generator config
    // If this is set, this will override the file format
    if (recordReaderClassName != null) {
      if (fileFormat != FileFormat.OTHER) {
        // NOTE: we currently have default file format set to AVRO inside segment generator config, do not want to break
        // this behavior for clients.
        LOGGER.warn("Using class: {} to read segment, ignoring configured file format: {}", recordReaderClassName,
            fileFormat);
      }
      return RecordReaderFactory.getRecordReaderByClass(recordReaderClassName, dataFile, sourceFields,
          segmentGeneratorConfig.getReaderConfig());
    }

    // NOTE: PinotSegmentRecordReader does not support time conversion (field spec must match)
    if (fileFormat == FileFormat.PINOT) {
      return new PinotSegmentRecordReader(dataFile, schema, segmentGeneratorConfig.getColumnSortOrder());
    } else {
      return RecordReaderFactory
          .getRecordReader(fileFormat, dataFile, sourceFields, segmentGeneratorConfig.getReaderConfig());
    }
  }

  public RecordReader getRecordReader() {
    return _recordReader;
  }

  public void init(SegmentGeneratorConfig config, RecordReader recordReader)
      throws Exception {
    SegmentCreationDataSource dataSource = new RecordReaderSegmentCreationDataSource(recordReader);
    init(config, dataSource, new TransformPipeline(config.getTableConfig(), config.getSchema()));
  }

  @Deprecated
  public void init(SegmentGeneratorConfig config, SegmentCreationDataSource dataSource,
      RecordTransformer recordTransformer, @Nullable ComplexTypeTransformer complexTypeTransformer)
      throws Exception {
    init(config, dataSource, new TransformPipeline(recordTransformer, complexTypeTransformer));
  }

  public void init(SegmentGeneratorConfig config, SegmentCreationDataSource dataSource,
      TransformPipeline transformPipeline)
      throws Exception {
    _config = config;
    _recordReader = dataSource.getRecordReader();
    _dataSchema = config.getSchema();
    _continueOnError = config.isContinueOnError();

    if (config.isFailOnEmptySegment()) {
      Preconditions.checkState(_recordReader.hasNext(), "No record in data source");
    }
    _transformPipeline = transformPipeline;
    // Use the same transform pipeline if the data source is backed by a record reader
    if (dataSource instanceof RecordReaderSegmentCreationDataSource) {
      ((RecordReaderSegmentCreationDataSource) dataSource).setTransformPipeline(transformPipeline);
    }

    // Initialize stats collection
    _segmentStats = dataSource.gatherStats(
        new StatsCollectorConfig(config.getTableConfig(), _dataSchema, config.getSegmentPartitionConfig()));
    _totalDocs = _segmentStats.getTotalDocCount();

    // Initialize index creation
    _segmentIndexCreationInfo = new SegmentIndexCreationInfo();
    _indexCreationInfoMap = new TreeMap<>();

    // Check if has star tree
    _indexCreator = new SegmentColumnarIndexCreator();

    // Ensure that the output directory exists
    final File indexDir = new File(config.getOutDir());
    if (!indexDir.exists()) {
      indexDir.mkdirs();
    }

    _ingestionSchemaValidator = SchemaValidatorFactory
        .getSchemaValidator(_dataSchema, _recordReader.getClass().getName(), config.getInputFilePath());

    // Create a temporary directory used in segment creation
    _tempIndexDir = new File(indexDir, "tmp-" + UUID.randomUUID());
    LOGGER.debug("tempIndexDir:{}", _tempIndexDir);
  }

  @Override
  public void build()
      throws Exception {
    // Count the number of documents and gather per-column statistics
    LOGGER.debug("Start building StatsCollector!");
    buildIndexCreationInfo();
    LOGGER.info("Finished building StatsCollector!");
    LOGGER.info("Collected stats for {} documents", _totalDocs);

    int incompleteRowsFound = 0;
    try {
      // Initialize the index creation using the per-column statistics information
      // TODO: _indexCreationInfoMap holds the reference to all unique values on heap (ColumnIndexCreationInfo ->
      //       ColumnStatistics) throughout the segment creation. Find a way to release the memory early.
      _indexCreator.init(_config, _segmentIndexCreationInfo, _indexCreationInfoMap, _dataSchema, _tempIndexDir);

      // Build the index
      _recordReader.rewind();
      LOGGER.info("Start building IndexCreator!");
      GenericRow reuse = new GenericRow();
      TransformPipeline.Result reusedResult = new TransformPipeline.Result();
      while (_recordReader.hasNext()) {
        long recordReadStartTime = System.currentTimeMillis();
        long recordReadStopTime = System.currentTimeMillis();
        long indexStopTime;
        reuse.clear();
        try {
          GenericRow decodedRow = _recordReader.next(reuse);
          recordReadStartTime = System.currentTimeMillis();
          _transformPipeline.processRow(decodedRow, reusedResult);
          recordReadStopTime = System.currentTimeMillis();
          _totalRecordReadTime += (recordReadStopTime - recordReadStartTime);
        } catch (Exception e) {
          if (!_continueOnError) {
            throw new RuntimeException("Error occurred while reading row during indexing", e);
          } else {
            incompleteRowsFound++;
            LOGGER.debug("Error occurred while reading row during indexing", e);
            continue;
          }
        }

        for (GenericRow row : reusedResult.getTransformedRows()) {
          _indexCreator.indexRow(row);
        }
        indexStopTime = System.currentTimeMillis();
        _totalIndexTime += (indexStopTime - recordReadStopTime);
        incompleteRowsFound += reusedResult.getIncompleteRowCount();
      }
    } catch (Exception e) {
      _indexCreator.close();
      throw e;
    } finally {
      _recordReader.close();
    }

    if (incompleteRowsFound > 0) {
      LOGGER.warn("Incomplete data found for {} records. This can be due to error during reader or transformations",
          incompleteRowsFound);
    }

    LOGGER.info("Finished records indexing in IndexCreator!");

    handlePostCreation();
  }

  private void handlePostCreation()
      throws Exception {
    ColumnStatistics timeColumnStatistics = _segmentStats.getColumnProfileFor(_config.getTimeColumnName());
    int sequenceId = _config.getSequenceId();
    if (timeColumnStatistics != null) {
      if (_totalDocs > 0) {
        _segmentName = _config.getSegmentNameGenerator()
            .generateSegmentName(sequenceId, timeColumnStatistics.getMinValue(), timeColumnStatistics.getMaxValue());
      } else {
        // When totalDoc is 0, check whether 'failOnEmptySegment' option is true. If so, directly fail the segment
        // creation.
        Preconditions.checkArgument(!_config.isFailOnEmptySegment(),
            "Failing the empty segment creation as the option 'failOnEmptySegment' is set to: " + _config
                .isFailOnEmptySegment());
        // Generate a unique name for a segment with no rows
        long now = System.currentTimeMillis();
        _segmentName = _config.getSegmentNameGenerator().generateSegmentName(sequenceId, now, now);
      }
    } else {
      _segmentName = _config.getSegmentNameGenerator().generateSegmentName(sequenceId, null, null);
    }

    try {
      // Write the index files to disk
      _indexCreator.setSegmentName(_segmentName);
      _indexCreator.seal();
    } finally {
      _indexCreator.close();
    }
    LOGGER.info("Finished segment seal!");

    // Delete the directory named after the segment name, if it exists
    final File outputDir = new File(_config.getOutDir());
    final File segmentOutputDir = new File(outputDir, _segmentName);
    if (segmentOutputDir.exists()) {
      FileUtils.deleteDirectory(segmentOutputDir);
    }

    // Move the temporary directory into its final location
    FileUtils.moveDirectory(_tempIndexDir, segmentOutputDir);

    // Delete the temporary directory
    FileUtils.deleteQuietly(_tempIndexDir);

    // Convert segment format if necessary
    convertFormatIfNecessary(segmentOutputDir);

    // Build star-tree V2 if necessary
    if (_totalDocs > 0) {
      buildStarTreeV2IfNecessary(segmentOutputDir);
    }

    Set<IndexType> postSegCreationIndexes = IndexService.getInstance().getAllIndexes().stream()
        .filter(indexType -> indexType.getIndexBuildLifecycle() == IndexType.IndexBuildLifecycle.POST_SEGMENT_CREATION)
        .collect(Collectors.toSet());

    if (postSegCreationIndexes.size() > 0) {
      // Build other indexes
      Map<String, Object> props = new HashMap<>();
      props.put(IndexLoadingConfig.READ_MODE_KEY, ReadMode.mmap);
      PinotConfiguration segmentDirectoryConfigs = new PinotConfiguration(props);

      SegmentDirectoryLoaderContext segmentLoaderContext =
          new SegmentDirectoryLoaderContext.Builder().setTableConfig(_config.getTableConfig())
              .setSchema(_config.getSchema()).setSegmentName(_segmentName)
              .setSegmentDirectoryConfigs(segmentDirectoryConfigs).build();

      IndexLoadingConfig indexLoadingConfig =
          new IndexLoadingConfig(null, _config.getTableConfig(), _config.getSchema());

      try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
          .load(segmentOutputDir.toURI(), segmentLoaderContext);
          SegmentDirectory.Writer segmentWriter = segmentDirectory.createWriter()) {
        for (IndexType indexType : postSegCreationIndexes) {
          IndexHandler handler =
              indexType.createIndexHandler(segmentDirectory, indexLoadingConfig.getFieldIndexConfigByColName(),
                  _config.getSchema(), _config.getTableConfig());
          handler.updateIndices(segmentWriter);
        }
      }
    }

    // Compute CRC and creation time
    long crc = CrcUtils.forAllFilesInFolder(segmentOutputDir).computeCrc();
    long creationTime;
    String creationTimeInConfig = _config.getCreationTime();
    if (creationTimeInConfig != null) {
      try {
        creationTime = Long.parseLong(creationTimeInConfig);
      } catch (Exception e) {
        LOGGER.error("Caught exception while parsing creation time in config, use current time as creation time");
        creationTime = System.currentTimeMillis();
      }
    } else {
      creationTime = System.currentTimeMillis();
    }

    // Persist creation metadata to disk
    persistCreationMeta(segmentOutputDir, crc, creationTime);

    LOGGER.info("Driver, record read time : {}", _totalRecordReadTime);
    LOGGER.info("Driver, stats collector time : {}", _totalStatsCollectorTime);
    LOGGER.info("Driver, indexing time : {}", _totalIndexTime);
  }

  private void buildStarTreeV2IfNecessary(File indexDir)
      throws Exception {
    List<StarTreeIndexConfig> starTreeIndexConfigs = _config.getStarTreeIndexConfigs();
    boolean enableDefaultStarTree = _config.isEnableDefaultStarTree();
    if (CollectionUtils.isNotEmpty(starTreeIndexConfigs) || enableDefaultStarTree) {
      MultipleTreesBuilder.BuildMode buildMode =
          _config.isOnHeap() ? MultipleTreesBuilder.BuildMode.ON_HEAP : MultipleTreesBuilder.BuildMode.OFF_HEAP;
      try (
          MultipleTreesBuilder builder = new MultipleTreesBuilder(starTreeIndexConfigs, enableDefaultStarTree, indexDir,
              buildMode)) {
        builder.build();
      }
    }
  }

  // Explanation of why we are using format converter:
  // There are 3 options to correctly generate segments to v3 format
  // 1. Generate v3 directly: This is efficient but v3 index writer needs to know buffer size upfront.
  // Inverted, star and raw indexes don't have the index size upfront. This is also least flexible approach
  // if we add more indexes in future.
  // 2. Hold data in-memory: One way to work around predeclaring sizes in (1) is to allocate "large" buffer (2GB?)
  // and hold the data in memory and write the buffer at the end. The memory requirement in this case increases linearly
  // with the number of columns. Variation of that is to mmap data to separate files...which is what we are doing here
  // 3. Another option is to generate dictionary and fwd indexes in v3 and generate inverted, star and raw indexes in
  // separate files. Then add those files to v3 index file. This leads to lot of hodgepodge code to
  // handle multiple segment formats.
  // Using converter is similar to option (2), plus it's battle-tested code. We will roll out with
  // this change to keep changes limited. Once we've migrated we can implement approach (1) with option to
  // copy for indexes for which we don't know sizes upfront.
  private void convertFormatIfNecessary(File segmentDirectory)
      throws Exception {
    SegmentVersion versionToGenerate = _config.getSegmentVersion();
    if (versionToGenerate.equals(SegmentVersion.v1)) {
      // v1 by default
      return;
    }
    SegmentFormatConverter converter = SegmentFormatConverterFactory.getConverter(SegmentVersion.v1, SegmentVersion.v3);
    converter.convert(segmentDirectory);
  }

  public ColumnStatistics getColumnStatisticsCollector(final String columnName)
      throws Exception {
    return _segmentStats.getColumnProfileFor(columnName);
  }

  public static void persistCreationMeta(File indexDir, long crc, long creationTime)
      throws IOException {
    File segmentDir = SegmentDirectoryPaths.findSegmentDirectory(indexDir);
    File creationMetaFile = new File(segmentDir, V1Constants.SEGMENT_CREATION_META);
    try (DataOutputStream output = new DataOutputStream(new FileOutputStream(creationMetaFile))) {
      output.writeLong(crc);
      output.writeLong(creationTime);
    }
  }

  /**
   * Complete the stats gathering process and store the stats information in indexCreationInfoMap.
   */
  void buildIndexCreationInfo()
      throws Exception {
    Set<String> varLengthDictionaryColumns = new HashSet<>(_config.getVarLengthDictionaryColumns());
    Set<String> rawIndexCreationColumns = _config.getRawIndexCreationColumns();
    Set<String> rawIndexCompressionTypeKeys = _config.getRawIndexCompressionType().keySet();
    for (FieldSpec fieldSpec : _dataSchema.getAllFieldSpecs()) {
      // Ignore virtual columns
      if (fieldSpec.isVirtualColumn()) {
        continue;
      }

      String columnName = fieldSpec.getName();
      DataType storedType = fieldSpec.getDataType().getStoredType();
      ColumnStatistics columnProfile = _segmentStats.getColumnProfileFor(columnName);
      boolean useVarLengthDictionary =
          shouldUseVarLengthDictionary(columnName, varLengthDictionaryColumns, storedType, columnProfile);
      Object defaultNullValue = fieldSpec.getDefaultNullValue();
      if (storedType == DataType.BYTES) {
        defaultNullValue = new ByteArray((byte[]) defaultNullValue);
      }
      boolean createDictionary = !rawIndexCreationColumns.contains(columnName)
          && !rawIndexCompressionTypeKeys.contains(columnName);
      _indexCreationInfoMap.put(columnName,
          new ColumnIndexCreationInfo(columnProfile, createDictionary, useVarLengthDictionary,
              false/*isAutoGenerated*/, defaultNullValue));
    }
    _segmentIndexCreationInfo.setTotalDocs(_totalDocs);
  }

  /**
   * Uses config and column properties like storedType and length of elements to determine if
   * varLengthDictionary should be used for a column
   * @deprecated Use
   * {@link DictionaryIndexType#shouldUseVarLengthDictionary(String, Set, DataType, ColumnStatistics)} instead.
   */
  @Deprecated
  public static boolean shouldUseVarLengthDictionary(String columnName, Set<String> varLengthDictColumns,
      DataType columnStoredType, ColumnStatistics columnProfile) {
    return DictionaryIndexType.shouldUseVarLengthDictionary(
        columnName, varLengthDictColumns, columnStoredType, columnProfile);
  }

  /**
   * Returns the name of the segment associated with this index creation driver.
   */
  @Override
  public String getSegmentName() {
    return _segmentName;
  }

  /**
   * Returns the path of the output directory
   */
  @Override
  public File getOutputDirectory() {
    return new File(new File(_config.getOutDir()), _segmentName);
  }

  /**
   * Returns the schema validator.
   */
  @Override
  public IngestionSchemaValidator getIngestionSchemaValidator() {
    return _ingestionSchemaValidator;
  }

  public SegmentPreIndexStatsContainer getSegmentStats() {
    return _segmentStats;
  }
}
