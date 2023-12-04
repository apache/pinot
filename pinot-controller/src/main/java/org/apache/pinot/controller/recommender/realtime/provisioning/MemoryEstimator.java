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
package org.apache.pinot.controller.recommender.realtime.provisioning;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.recommender.data.DataGenerationHelpers;
import org.apache.pinot.controller.recommender.data.generator.DataGenerator;
import org.apache.pinot.controller.recommender.data.generator.DataGeneratorSpec;
import org.apache.pinot.controller.recommender.io.metadata.DateTimeFieldSpecMetadata;
import org.apache.pinot.controller.recommender.io.metadata.FieldMetadata;
import org.apache.pinot.controller.recommender.io.metadata.SchemaWithMetaData;
import org.apache.pinot.controller.recommender.io.metadata.TimeFieldSpecMetadata;
import org.apache.pinot.controller.recommender.io.metadata.TimeGranularitySpecMetadata;
import org.apache.pinot.plugin.inputformat.csv.CSVRecordReaderConfig;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.io.readerwriter.RealtimeIndexOffHeapMemoryManager;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Given a sample segment, this class can estimate how much memory would be used per host, for various combinations
 * of numHostsToProvision and numHoursToConsume
 */
public class MemoryEstimator {

  public static final String NOT_APPLICABLE = "NA";
  private static final String STATS_FILE_NAME = "stats.ser";
  private static final String STATS_FILE_COPY_NAME = "stats.copy.ser";

  private final TableConfig _tableConfig;
  private final String _tableNameWithType;
  private final Schema _schema;
  private final File _sampleCompletedSegment;
  private final long _sampleSegmentConsumedSeconds;
  private final int _totalDocsInSampleSegment;
  private final long _maxUsableHostMemory;
  private final int _tableRetentionHours;

  private final SegmentMetadataImpl _segmentMetadata;
  private final long _sampleCompletedSegmentSizeBytes;
  int _avgMultiValues;

  // Working dir will contain statsFile and also the generated segment if requested.
  // It will get deleted after memory estimation is done.
  private final File _workingDir;

  private String[][] _activeMemoryPerHost;
  private String[][] _optimalSegmentSize;
  private String[][] _numRowsInSegment;
  private String[][] _consumingMemoryPerHost;
  private String[][] _numSegmentsQueriedPerHost;

  /**
   * Constructor used for processing the given completed segment
   */
  public MemoryEstimator(TableConfig tableConfig, Schema schema, File sampleCompletedSegment,
      double ingestionRatePerPartition, long maxUsableHostMemory, int tableRetentionHours, File workingDir) {
    _maxUsableHostMemory = maxUsableHostMemory;
    _tableConfig = tableConfig;
    _tableNameWithType = tableConfig.getTableName();
    _schema = schema;
    _sampleCompletedSegment = sampleCompletedSegment;
    _tableRetentionHours = tableRetentionHours;

    _sampleCompletedSegmentSizeBytes = FileUtils.sizeOfDirectory(_sampleCompletedSegment);
    try {
      _segmentMetadata = new SegmentMetadataImpl(_sampleCompletedSegment);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception when reading segment index dir", e);
    }
    _totalDocsInSampleSegment = _segmentMetadata.getTotalDocs();
    _sampleSegmentConsumedSeconds = (int) (_totalDocsInSampleSegment / ingestionRatePerPartition);

    _avgMultiValues = getAvgMultiValues();
    _workingDir = workingDir;
  }

  /**
   * Constructor used for processing the given data characteristics (instead of completed segment)
   */
  public MemoryEstimator(TableConfig tableConfig, Schema schema, SchemaWithMetaData schemaWithMetadata,
      int numberOfRows, double ingestionRatePerPartition, long maxUsableHostMemory, int tableRetentionHours,
      File workingDir) {
    this(tableConfig, schema,
        generateCompletedSegment(schemaWithMetadata, schema, tableConfig, numberOfRows, workingDir),
        ingestionRatePerPartition, maxUsableHostMemory, tableRetentionHours, workingDir);
  }

  /**
   * Initialize the stats file using the sample segment provided.
   * <br>This involves indexing each row of the sample segment using MutableSegmentImpl. This is equivalent to
   * consuming the rows of a segment.
   * Although they will be in a different order than consumed by the host, the stats should be equivalent.
   * <br>Invoking a destroy on the MutableSegmentImpl at the end will dump the collected stats into the stats.ser
   * file provided in the statsHistory.
   */
  public File initializeStatsHistory() {

    File statsFile = new File(_workingDir, STATS_FILE_NAME);
    RealtimeSegmentStatsHistory sampleStatsHistory;
    try {
      sampleStatsHistory = RealtimeSegmentStatsHistory.deserialzeFrom(statsFile);
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(
          "Exception when deserializing stats history from stats file " + statsFile.getAbsolutePath(), e);
    }

    RealtimeIndexOffHeapMemoryManager memoryManager = new DirectMemoryManager(_segmentMetadata.getName());
    SegmentZKMetadata segmentZKMetadata = getSegmentZKMetadata(_segmentMetadata, _segmentMetadata.getTotalDocs());

    // create a config
    RealtimeSegmentConfig.Builder realtimeSegmentConfigBuilder =
        new RealtimeSegmentConfig.Builder(_tableConfig, _schema).setTableNameWithType(_tableNameWithType)
            .setSegmentName(_segmentMetadata.getName()).setStreamName(_tableNameWithType)
            .setSchema(_segmentMetadata.getSchema()).setCapacity(_segmentMetadata.getTotalDocs())
            .setAvgNumMultiValues(_avgMultiValues).setSegmentZKMetadata(segmentZKMetadata).setOffHeap(true)
            .setMemoryManager(memoryManager).setStatsHistory(sampleStatsHistory);

    // create mutable segment impl
    MutableSegmentImpl mutableSegmentImpl = new MutableSegmentImpl(realtimeSegmentConfigBuilder.build(), null);

    // read all rows and index them
    try (PinotSegmentRecordReader segmentRecordReader = new PinotSegmentRecordReader(_sampleCompletedSegment)) {
      GenericRow row = new GenericRow();
      while (segmentRecordReader.hasNext()) {
        row = segmentRecordReader.next(row);
        mutableSegmentImpl.index(row, null);
        row.clear();
      }
    } catch (Exception e) {
      throw new RuntimeException("Caught exception when indexing rows");
    }

    // dump stats into stats file
    mutableSegmentImpl.destroy();

    return statsFile;
  }

  /**
   * Given a sample segment, the time for which it consumed, numReplicas and numPartitions, estimate how much memory
   * would be required per host for this table
   * <br>
   * <br>Algorithm:
   * <br>Given numReplicas and numPartitions, we can find out total consuming partitions per host, for various numHosts
   * <br><b>totalConsumingPartitionsPerHost = (numReplicas * numPartitions)/numHosts</b>
   * <br>
   * <br>Given a sample realtime completed segment (with size s), and how long it consumed for (t),
   * <br>we can estimate how much memory the table would require for various combinations of num hosts and num hours
   * <br>
   * <br>For estimating the memory occupied by completed segments-
   * <br>For each numHoursToConsume we compute:
   * <br>If a segment with size s takes time t to complete, then consuming for time numHoursToConsume would create
   * segment with size <b>estimatedSize = (numHoursToConsume/t)*s</b>
   * <br>If retention for completed segments in memory is rt hours, then the segment would be in memory for <b>
   *   (rt-numHoursToConsume) hours
   * </b>
   * <br>A segment would complete every numHoursToConsume hours, so we would have at a time
   * <b>numCompletedSegmentsAtATime = (rt-numHoursToConsume)/numHoursToConsume</b> to hold in memory
   * <br>As a result, <b>totalCompletedSegmentsMemory per ConsumingPartition = estimatedSize *
   * numCompletedSegmentsAtATime</b>
   * <br>
   * <br>For estimating the memory occupied by consuming segments-
   * <br>Using the sample segment, we initialize the stats history
   * <br>For each numHoursToConsume we compute:
   * <br>If totalDocs in sample segment is n when it consumed for time t, then consuming for time numHoursToConsume
   * would create <b>totalDocs = (numHoursToConsume/t)*n</b>
   * <br>We create a {@link MutableSegmentImpl} using the totalDocs, and then fetch the memory used by the memory
   * manager, to get totalConsumingSegmentMemory per ConsumingPartition
   * <br>
   * <br><b>totalMemory = (totalCompletedMemory per ConsumingPartition + totalConsumingMemory per ConsumingPartition)
   * * totalConsumingPartitionsPerHost</b>
   * <br>
   * @param statsFile stats file from a sample segment for the same table
   * @param numHosts list of number of hosts that are to be provisioned
   * @param numHours list of number of hours to be consumed
   * @param totalConsumingPartitions total consuming partitions we are provisioning for
   * @param retentionHours number of most recent hours to be retained in memory for queries.
   * @throws IOException
   */
  public void estimateMemoryUsed(File statsFile, int[] numHosts, int[] numHours, final int totalConsumingPartitions,
      final int retentionHours)
      throws IOException {
    _activeMemoryPerHost = new String[numHours.length][numHosts.length];
    _optimalSegmentSize = new String[numHours.length][numHosts.length];
    _numRowsInSegment = new String[numHours.length][numHosts.length];
    _consumingMemoryPerHost = new String[numHours.length][numHosts.length];
    _numSegmentsQueriedPerHost = new String[numHours.length][numHosts.length];
    for (int i = 0; i < numHours.length; i++) {
      for (int j = 0; j < numHosts.length; j++) {
        _activeMemoryPerHost[i][j] = NOT_APPLICABLE;
        _consumingMemoryPerHost[i][j] = NOT_APPLICABLE;
        _numRowsInSegment[i][j] = NOT_APPLICABLE;
        _optimalSegmentSize[i][j] = NOT_APPLICABLE;
        _numSegmentsQueriedPerHost[i][j] = NOT_APPLICABLE;
      }
    }

    try {
      int invertedColumnsCount = countInvertedColumns();

      for (int i = 0; i < numHours.length; i++) {
        int numHoursToConsume = numHours[i];
        if (numHoursToConsume > retentionHours) {
          continue;
        }
        long secondsToConsume = numHoursToConsume * 3600L;
        // consuming for _numHoursSampleSegmentConsumed, gives size sampleCompletedSegmentSizeBytes
        // hence, consuming for numHoursToConsume would give:
        long completedSegmentSizeBytes =
            (long) (((double) secondsToConsume / _sampleSegmentConsumedSeconds) * _sampleCompletedSegmentSizeBytes);

        // numHoursSampleSegmentConsumed created totalDocsInSampleSegment num rows
        // numHoursToConsume will create ? rows
        int totalDocs = (int) (((double) secondsToConsume / _sampleSegmentConsumedSeconds) * _totalDocsInSampleSegment);
        long memoryForConsumingSegmentPerPartition = getMemoryForConsumingSegmentPerPartition(statsFile, totalDocs);

        memoryForConsumingSegmentPerPartition += getMemoryForInvertedIndex(
            memoryForConsumingSegmentPerPartition, invertedColumnsCount);

        int numActiveSegmentsPerPartition = (retentionHours + numHoursToConsume - 1) / numHoursToConsume;
        long activeMemoryForCompletedSegmentsPerPartition =
            completedSegmentSizeBytes * (numActiveSegmentsPerPartition - 1);
        int numCompletedSegmentsPerPartition = (_tableRetentionHours + numHoursToConsume - 1) / numHoursToConsume - 1;

        for (int j = 0; j < numHosts.length; j++) {
          int numHostsToProvision = numHosts[j];
          // adjustment because we want ceiling of division and not floor, as some hosts will have an extra partition
          // due to the remainder of the division
          int totalConsumingPartitionsPerHost =
              (totalConsumingPartitions + numHostsToProvision - 1) / numHostsToProvision;

          long activeMemoryForCompletedSegmentsPerHost =
              activeMemoryForCompletedSegmentsPerPartition * totalConsumingPartitionsPerHost;
          long totalMemoryForConsumingSegmentsPerHost =
              memoryForConsumingSegmentPerPartition * totalConsumingPartitionsPerHost;
          long activeMemoryPerHostBytes =
              activeMemoryForCompletedSegmentsPerHost + totalMemoryForConsumingSegmentsPerHost;
          long mappedMemoryPerHost = totalMemoryForConsumingSegmentsPerHost + (numCompletedSegmentsPerPartition
              * totalConsumingPartitionsPerHost * completedSegmentSizeBytes);

          if (activeMemoryPerHostBytes <= _maxUsableHostMemory) {
            _activeMemoryPerHost[i][j] =
                DataSizeUtils.fromBytes(activeMemoryPerHostBytes) + "/" + DataSizeUtils.fromBytes(mappedMemoryPerHost);
            _consumingMemoryPerHost[i][j] = DataSizeUtils.fromBytes(totalMemoryForConsumingSegmentsPerHost);
            _optimalSegmentSize[i][j] = DataSizeUtils.fromBytes(completedSegmentSizeBytes);
            _numRowsInSegment[i][j] = String.valueOf(totalDocs);
            _numSegmentsQueriedPerHost[i][j] =
                String.valueOf(numActiveSegmentsPerPartition * totalConsumingPartitionsPerHost);
          }
        }
      }
    } finally {
      // cleanup
      FileUtils.deleteQuietly(_workingDir);
    }
  }

  private long getMemoryForConsumingSegmentPerPartition(File statsFile, int totalDocs)
      throws IOException {
    // We don't want the stats history to get updated from all our dummy runs
    // So we copy over the original stats history every time we start
    File statsFileCopy = new File(_workingDir, STATS_FILE_COPY_NAME);
    FileUtils.copyFile(statsFile, statsFileCopy);
    RealtimeSegmentStatsHistory statsHistory;
    try {
      statsHistory = RealtimeSegmentStatsHistory.deserialzeFrom(statsFileCopy);
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(
          "Exception when deserializing stats history from stats file " + statsFileCopy.getAbsolutePath(), e);
    }
    RealtimeIndexOffHeapMemoryManager memoryManager = new DirectMemoryManager(_segmentMetadata.getName());
    SegmentZKMetadata segmentZKMetadata = getSegmentZKMetadata(_segmentMetadata, totalDocs);

    RealtimeSegmentConfig.Builder realtimeSegmentConfigBuilder =
        new RealtimeSegmentConfig.Builder(_tableConfig, _schema).setTableNameWithType(_tableNameWithType)
            .setSegmentName(_segmentMetadata.getName()).setStreamName(_tableNameWithType)
            .setSchema(_segmentMetadata.getSchema()).setCapacity(totalDocs).setAvgNumMultiValues(_avgMultiValues)
            .setSegmentZKMetadata(segmentZKMetadata).setOffHeap(true)
            .setMemoryManager(memoryManager).setStatsHistory(statsHistory);

    // create mutable segment impl
    MutableSegmentImpl mutableSegmentImpl = new MutableSegmentImpl(realtimeSegmentConfigBuilder.build(), null);
    long memoryForConsumingSegmentPerPartition = memoryManager.getTotalAllocatedBytes();
    mutableSegmentImpl.destroy();
    FileUtils.deleteQuietly(statsFileCopy);
    return memoryForConsumingSegmentPerPartition;
  }

  /**
   * Gets the average num multivalues across all multi value columns in the data
   * @return
   */
  private int getAvgMultiValues() {
    int avgMultiValues = 0;
    Set<String> multiValueColumns =
        _segmentMetadata.getSchema().getAllFieldSpecs().stream().filter(fieldSpec -> !fieldSpec.isSingleValueField())
            .map(FieldSpec::getName).collect(Collectors.toSet());

    if (!multiValueColumns.isEmpty()) {

      int numValues = 0;
      long multiValuesSum = 0;
      try {
        PinotSegmentRecordReader segmentRecordReader = new PinotSegmentRecordReader(_sampleCompletedSegment);
        GenericRow row = new GenericRow();

        while (segmentRecordReader.hasNext()) {
          row = segmentRecordReader.next(row);
          for (String multiValueColumn : multiValueColumns) {
            multiValuesSum += ((Object[]) (row.getValue(multiValueColumn))).length;
            numValues++;
          }
          row.clear();
        }
      } catch (Exception e) {
        throw new RuntimeException("Caught exception when calculating avg multi values");
      }
      avgMultiValues = (int) (((double) multiValuesSum + numValues - 1) / numValues);
    }
    return avgMultiValues;
  }

  /**
   * Computes the memory by the inverted indexes in the consuming segment
   * This is just an estimation. We use MutableRoaringBitmap for inverted indexes, which use heap memory.
   * @param totalMemoryForConsumingSegment
   * @return
   */
  private long getMemoryForInvertedIndex(long totalMemoryForConsumingSegment, int invertedColumnsCount) {
    // TODO: better way to estimate inverted indexes memory utilization
    long totalInvertedIndexSizeBytes = 0;
    if (invertedColumnsCount > 0) {
      long memoryForEachColumn = totalMemoryForConsumingSegment / _segmentMetadata.getAllColumns().size();
      totalInvertedIndexSizeBytes = (long) (memoryForEachColumn * 0.3 * invertedColumnsCount);
    }
    return totalInvertedIndexSizeBytes;
  }

  private int countInvertedColumns() {
    Map<String, IndexConfig> invertedConfig = StandardIndexes.inverted().getConfig(_tableConfig, _schema);
    return (int) invertedConfig.values().stream()
        .filter(IndexConfig::isEnabled)
        .count();
  }

  /**
   * Creates a sample segment ZK metadata for the given segment metadata
   * @param segmentMetadata
   * @return
   */
  private SegmentZKMetadata getSegmentZKMetadata(SegmentMetadataImpl segmentMetadata, int totalDocs) {
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentMetadata.getName());
    segmentZKMetadata.setStartTime(segmentMetadata.getStartTime());
    segmentZKMetadata.setEndTime(segmentMetadata.getEndTime());
    segmentZKMetadata.setTimeUnit(segmentMetadata.getTimeUnit());
    segmentZKMetadata.setCreationTime(segmentMetadata.getIndexCreationTime());
    segmentZKMetadata.setTotalDocs(totalDocs);
    segmentZKMetadata.setCrc(Long.parseLong(segmentMetadata.getCrc()));
    return segmentZKMetadata;
  }

  /**
   * Given the memory required by a completed segment, this method calculates the total memory required by completed
   * segments at a time for a partition.
   * This calculation takes into account the number of hours the completed segments need to be retained (configured
   * retention - numHoursToConsume)
   * It also takes into account that a new segment will be created every numHoursToConsume hours, and so we might
   * need to keep multiple completed segments in memory at a time
   * @param completedSegmentSizeBytes
   * @param numHoursToConsume
   * @return
   */
  private long calculateMemoryForCompletedSegmentsPerPartition(long completedSegmentSizeBytes, int numHoursToConsume,
      int retentionHours) {

    int numSegmentsInMemory = (retentionHours + numHoursToConsume - 1) / numHoursToConsume;
    return completedSegmentSizeBytes * (numSegmentsInMemory - 1);
  }

  public String[][] getActiveMemoryPerHost() {
    return _activeMemoryPerHost;
  }

  public String[][] getOptimalSegmentSize() {
    return _optimalSegmentSize;
  }

  public String[][] getNumRowsInSegment() {
    return _numRowsInSegment;
  }

  public String[][] getConsumingMemoryPerHost() {
    return _consumingMemoryPerHost;
  }

  public String[][] getNumSegmentsQueriedPerHost() {
    return _numSegmentsQueriedPerHost;
  }

  private static File generateCompletedSegment(SchemaWithMetaData schemaWithMetadata, Schema schema,
      TableConfig tableConfig, int numberOfRows, File workingDir) {
    return new SegmentGenerator(schemaWithMetadata, schema, tableConfig, numberOfRows, true, workingDir).generate();
  }

  /**
   * This class is used in Memory Estimator to generate segment based on the the given characteristics of data
   */
  public static class SegmentGenerator {
    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentGenerator.class);

    private SchemaWithMetaData _schemaWithMetadata;
    private Schema _schema;
    private TableConfig _tableConfig;
    private int _numberOfRows;
    private boolean _deleteCsv;
    private File _workingDir;

    public SegmentGenerator(SchemaWithMetaData schemaWithMetadata, Schema schema, TableConfig tableConfig,
        int numberOfRows, boolean deleteCsv, File workingDir) {
      _schemaWithMetadata = schemaWithMetadata;
      _schema = schema;
      _tableConfig = tableConfig;
      _numberOfRows = numberOfRows;
      _deleteCsv = deleteCsv;
      _workingDir = workingDir;
    }

    public File generate() {
      File csvDataFile = generateData();
      File segment = createSegment(csvDataFile);
      if (_deleteCsv) {
        File csvDir = csvDataFile.getParentFile();
        FileUtils.deleteQuietly(csvDir);
      }
      return segment;
    }

    private File generateData() {

      // create maps of "column name" to ...
      Map<String, Integer> lengths = new HashMap<>();
      Map<String, Double> mvCounts = new HashMap<>();
      Map<String, Integer> cardinalities = new HashMap<>();
      Map<String, FieldSpec.DataType> dataTypes = new HashMap<>();
      Map<String, FieldSpec.FieldType> fieldTypes = new HashMap<>();
      Map<String, TimeUnit> timeUnits = new HashMap<>();
      List<String> colNames = new ArrayList<>();
      List<FieldMetadata> dimensions = _schemaWithMetadata.getDimensionFieldSpecs();
      List<FieldMetadata> metrics = _schemaWithMetadata.getMetricFieldSpecs();
      List<DateTimeFieldSpecMetadata> dateTimes = _schemaWithMetadata.getDateTimeFieldSpecs();
      Stream.concat(Stream.concat(dimensions.stream(), metrics.stream()), dateTimes.stream()).forEach(column -> {
        String name = column.getName();
        colNames.add(name);
        lengths.put(name, column.getAverageLength());
        mvCounts.put(name, column.getNumValuesPerEntry());
        cardinalities.put(name, column.getCardinality());
        dataTypes.put(name, column.getDataType());
        fieldTypes.put(name, column.getFieldType());
      });
      dateTimes.forEach(dateTimeColumn -> {
        TimeUnit timeUnit = new DateTimeFormatSpec(dateTimeColumn.getFormat()).getColumnUnit();
        timeUnits.put(dateTimeColumn.getName(), timeUnit);
      });
      TimeFieldSpecMetadata timeSpec = _schemaWithMetadata.getTimeFieldSpec();
      if (timeSpec != null) {
        String name = timeSpec.getName();
        colNames.add(name);
        cardinalities.put(name, timeSpec.getCardinality());
        dataTypes.put(name, timeSpec.getDataType());
        fieldTypes.put(name, timeSpec.getFieldType());
        TimeGranularitySpecMetadata timeGranSpec =
            timeSpec.getOutgoingGranularitySpec() != null ? timeSpec.getOutgoingGranularitySpec()
                : timeSpec.getIncomingGranularitySpec();
        timeUnits.put(name, timeGranSpec.getTimeType());
      }

      // generate data
      String outputDir = new File(_workingDir, "csv").getAbsolutePath();
      DataGeneratorSpec spec =
          new DataGeneratorSpec(colNames, cardinalities, new HashMap<>(), new HashMap<>(), mvCounts, lengths, dataTypes,
              fieldTypes, timeUnits);
      DataGenerator dataGenerator = new DataGenerator();
      try {
        dataGenerator.init(spec);
        DataGenerationHelpers.generateCsv(dataGenerator, _numberOfRows, 1, outputDir, true);
        File outputFile = Paths.get(outputDir, "output_0.csv").toFile();
        LOGGER.info("Successfully generated data file: {}", outputFile);
        return outputFile;
      } catch (Exception e) {
        FileUtils.deleteQuietly(new File(outputDir));
        throw new RuntimeException(e);
      }
    }

    private File createSegment(File csvDataFile) {

      // create segment
      LOGGER.info("Started creating segment from file: {}", csvDataFile);
      String outDir = new File(_workingDir, "segment").getAbsolutePath();
      SegmentGeneratorConfig segmentGeneratorConfig = getSegmentGeneratorConfig(csvDataFile, outDir);
      SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
      try {
        driver.init(segmentGeneratorConfig);
        driver.build();
      } catch (Exception e) {
        FileUtils.deleteQuietly(new File(outDir));
        File csvDir = csvDataFile.getParentFile();
        FileUtils.deleteQuietly(csvDir);
        throw new RuntimeException("Caught exception while generating segment from file: " + csvDataFile, e);
      }
      String segmentName = driver.getSegmentName();
      File indexDir = new File(outDir, segmentName);
      LOGGER.info("Successfully created segment: {} at directory: {}", segmentName, indexDir);

      // verify segment
      LOGGER.info("Verifying the segment by loading it");
      ImmutableSegment segment;
      try {
        segment = ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while verifying the created segment", e);
      }
      LOGGER.info("Successfully loaded segment: {} of size: {} bytes", segmentName, segment.getSegmentSizeBytes());
      segment.destroy();

      return indexDir;
    }

    private SegmentGeneratorConfig getSegmentGeneratorConfig(File csvDataFile, String outDir) {
      SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(_tableConfig, _schema);
      segmentGeneratorConfig.setInputFilePath(csvDataFile.getPath());
      segmentGeneratorConfig.setFormat(FileFormat.CSV);
      segmentGeneratorConfig.setOutDir(outDir);
      segmentGeneratorConfig.setTableName(_tableConfig.getTableName());
      segmentGeneratorConfig.setSequenceId(0);

      CSVRecordReaderConfig recordReaderConfig = new CSVRecordReaderConfig();
      recordReaderConfig.setEscapeCharacter('\\');
      segmentGeneratorConfig.setReaderConfig(recordReaderConfig);

      return segmentGeneratorConfig;
    }
  }
}
