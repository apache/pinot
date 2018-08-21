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
package com.linkedin.pinot.tools.realtime.provisioning;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.utils.DataSize;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.PinotSegmentRecordReader;
import com.linkedin.pinot.core.indexsegment.mutable.MutableSegmentImpl;
import com.linkedin.pinot.core.io.readerwriter.RealtimeIndexOffHeapMemoryManager;
import com.linkedin.pinot.core.io.writer.impl.DirectMemoryManager;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentConfig;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentStatsHistory;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;


/**
 * Given a sample segment, this class can estimate how much memory would be used per host, for various combinations of numHostsToProvision and numHoursToConsume
 */
public class MemoryEstimator {

  private static final long MAX_MEMORY_BYTES = DataSize.toBytes("48G");
  private static final String NOT_APPLICABLE = "NA";
  private static final String TMP_DIR = System.getProperty("java.io.tmpdir") + File.separator;
  private static final String STATS_FILE_NAME = "stats.ser";
  private static final String STATS_FILE_COPY_NAME = "stats.copy.ser";

  private TableConfig _tableConfig;
  private File _sampleCompletedSegment;
  private long _sampleSegmentConsumedSeconds;

  private SegmentMetadataImpl _segmentMetadata;
  private long _sampleCompletedSegmentSizeBytes;
  private Set<String> _invertedIndexColumns = new HashSet<>();
  private Set<String> _noDictionaryColumns = new HashSet<>();
  int _avgMultiValues;
  private File _tableDataDir;

  private String[][] _totalMemoryPerHost;
  private String[][] _optimalSegmentSize;
  private String[][] _consumingMemoryPerHost;

  public MemoryEstimator(TableConfig tableConfig, File sampleCompletedSegment, long sampleSegmentConsumedSeconds) {
    _tableConfig = tableConfig;
    _sampleCompletedSegment = sampleCompletedSegment;
    _sampleSegmentConsumedSeconds = sampleSegmentConsumedSeconds;

    _sampleCompletedSegmentSizeBytes = FileUtils.sizeOfDirectory(_sampleCompletedSegment);
    try {
      _segmentMetadata = new SegmentMetadataImpl(_sampleCompletedSegment);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception when reading segment index dir", e);
    }

    if (CollectionUtils.isNotEmpty(_tableConfig.getIndexingConfig().getNoDictionaryColumns())) {
      _noDictionaryColumns.addAll(_tableConfig.getIndexingConfig().getNoDictionaryColumns());
    }
    if (CollectionUtils.isNotEmpty(_tableConfig.getIndexingConfig().getInvertedIndexColumns())) {
      _invertedIndexColumns.addAll(_tableConfig.getIndexingConfig().getInvertedIndexColumns());
    }
    _avgMultiValues = getAvgMultiValues();

    _tableDataDir = new File(TMP_DIR, _segmentMetadata.getTableName());
    try {
      FileUtils.deleteDirectory(_tableDataDir);
    } catch (IOException e) {
      throw new RuntimeException("Exception in deleting directory " + _tableDataDir.getAbsolutePath(), e);
    }
    _tableDataDir.mkdir();
  }

  /**
   * Initialize the stats file using the sample segment provided.
   * <br>This involves indexing each row of the sample segment using MutableSegmentImpl. This is equivalent to consuming the rows of a segment.
   * Although they will be in a different order than consumed by the host, the stats should be equivalent.
   * <br>Invoking a destroy on the MutableSegmentImpl at the end will dump the collected stats into the stats.ser file provided in the statsHistory.
   */
  public File initializeStatsHistory() {

    File statsFile = new File(_tableDataDir, STATS_FILE_NAME);
    RealtimeSegmentStatsHistory sampleStatsHistory;
    try {
      sampleStatsHistory = RealtimeSegmentStatsHistory.deserialzeFrom(statsFile);
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(
          "Exception when deserializing stats history from stats file " + statsFile.getAbsolutePath(), e);
    }

    RealtimeIndexOffHeapMemoryManager memoryManager = new DirectMemoryManager(_segmentMetadata.getName());
    RealtimeSegmentZKMetadata segmentZKMetadata =
        getRealtimeSegmentZKMetadata(_segmentMetadata, _segmentMetadata.getTotalRawDocs());

    // create a config
    RealtimeSegmentConfig.Builder realtimeSegmentConfigBuilder =
        new RealtimeSegmentConfig.Builder().setSegmentName(_segmentMetadata.getName())
            .setStreamName(_segmentMetadata.getTableName())
            .setSchema(_segmentMetadata.getSchema())
            .setCapacity(_segmentMetadata.getTotalDocs())
            .setAvgNumMultiValues(_avgMultiValues)
            .setNoDictionaryColumns(_noDictionaryColumns)
            .setInvertedIndexColumns(_invertedIndexColumns)
            .setRealtimeSegmentZKMetadata(segmentZKMetadata)
            .setOffHeap(true)
            .setMemoryManager(memoryManager)
            .setStatsHistory(sampleStatsHistory);

    // create mutable segment impl
    MutableSegmentImpl mutableSegmentImpl = new MutableSegmentImpl(realtimeSegmentConfigBuilder.build());

    // read all rows and index them
    try (PinotSegmentRecordReader segmentRecordReader = new PinotSegmentRecordReader(_sampleCompletedSegment);) {
      GenericRow row = new GenericRow();
      while (segmentRecordReader.hasNext()) {
        segmentRecordReader.next(row);
        mutableSegmentImpl.index(row);
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
   * Given a sample segment, the time for which it consumed, numReplicas and numPartitions, estimate how much memory would be required per host for this table
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
   * <br>If a segment with size s takes time t to complete, then consuming for time numHoursToConsume would create segment with size <b>estimatedSize = (numHoursToConsume/t)*s</b>
   * <br>If retention for completed segments in memory is rt hours, then the segment would be in memory for <b>(rt-numHoursToConsume) hours</b>
   * <br>A segment would complete every numHoursToConsume hours, so we would have at a time <b>numCompletedSegmentsAtATime = (rt-numHoursToConsume)/numHoursToConsume</b> to hold in memory
   * <br>As a result, <b>totalCompletedSegmentsMemory per ConsumingPartition = estimatedSize * numCompletedSegmentsAtATime</b>
   * <br>
   * <br>For estimating the memory occupied by consuming segments-
   * <br>Using the sample segment, we initialize the stats history
   * <br>For each numHoursToConsume we compute:
   * <br>If totalDocs in sample segment is n when it consumed for time t, then consuming for time numHoursToConsume would create <b>totalDocs = (numHoursToConsume/t)*n</b>
   * <br>We create a {@link MutableSegmentImpl} using the totalDocs, and then fetch the memory used by the memory manager, to get totalConsumingSegmentMemory per ConsumingPartition
   * <br>
   * <br><b>totalMemory = (totalCompletedMemory per ConsumingPartition + totalConsumingMemory per ConsumingPartition) * totalConsumingPartitionsPerHost</b>
   * <br>
   * @param statsFile stats file from a sample segment for the same table
   * @param numHosts list of number of hosts that are to be provisioned
   * @param numHours list of number of hours to be consumed
   * @param totalConsumingPartitions total consuming partitions we are provisioning for
   * @param retentionHours what is the amount of retention in memory expected for completed segments
   * @throws IOException
   */
  public void estimateMemoryUsed(File statsFile, int[] numHosts, int[] numHours, int totalConsumingPartitions,
      int retentionHours) throws IOException {
    _totalMemoryPerHost = new String[numHours.length][numHosts.length];
    _optimalSegmentSize = new String[numHours.length][numHosts.length];
    _consumingMemoryPerHost = new String[numHours.length][numHosts.length];

    for (int i = 0; i < numHours.length; i++) {
      int numHoursToConsume = numHours[i];
      long secondsToConsume = numHoursToConsume * 3600;
      // consuming for _numHoursSampleSegmentConsumed, gives size sampleCompletedSegmentSizeBytes
      // hence, consuming for numHoursToConsume would give:
      long completedSegmentSizeBytes =
          (long) (((double) secondsToConsume / _sampleSegmentConsumedSeconds) * _sampleCompletedSegmentSizeBytes);

      long totalMemoryForCompletedSegmentsPerPartition =
          calculateMemoryForCompletedSegmentsPerPartition(completedSegmentSizeBytes, numHoursToConsume, retentionHours);

      int totalDocsInSampleSegment = _segmentMetadata.getTotalDocs();
      // numHoursSampleSegmentConsumed created totalDocsInSampleSegment num rows
      // numHoursToConsume will create ? rows
      int totalDocs = (int) (((double) secondsToConsume / _sampleSegmentConsumedSeconds) * totalDocsInSampleSegment);

      // We don't want the stats history to get updated from all our dummy runs
      // So we copy over the original stats history every time we start
      File statsFileCopy = new File(_tableDataDir, STATS_FILE_COPY_NAME);
      FileUtils.copyFile(statsFile, statsFileCopy);
      RealtimeSegmentStatsHistory statsHistory;
      try {
        statsHistory = RealtimeSegmentStatsHistory.deserialzeFrom(statsFileCopy);
      } catch (IOException | ClassNotFoundException e) {
        throw new RuntimeException(
            "Exception when deserializing stats history from stats file " + statsFileCopy.getAbsolutePath(), e);
      }
      RealtimeIndexOffHeapMemoryManager memoryManager = new DirectMemoryManager(_segmentMetadata.getName());
      RealtimeSegmentZKMetadata segmentZKMetadata = getRealtimeSegmentZKMetadata(_segmentMetadata, totalDocs);

      RealtimeSegmentConfig.Builder realtimeSegmentConfigBuilder =
          new RealtimeSegmentConfig.Builder().setSegmentName(_segmentMetadata.getName())
              .setStreamName(_segmentMetadata.getTableName())
              .setSchema(_segmentMetadata.getSchema())
              .setCapacity(totalDocs)
              .setAvgNumMultiValues(_avgMultiValues)
              .setNoDictionaryColumns(_noDictionaryColumns)
              .setInvertedIndexColumns(_invertedIndexColumns)
              .setRealtimeSegmentZKMetadata(segmentZKMetadata)
              .setOffHeap(true)
              .setMemoryManager(memoryManager)
              .setStatsHistory(statsHistory);

      // create mutable segment impl
      MutableSegmentImpl mutableSegmentImpl = new MutableSegmentImpl(realtimeSegmentConfigBuilder.build());
      long memoryForConsumingSegmentPerPartition = memoryManager.getTotalAllocatedBytes();
      mutableSegmentImpl.destroy();
      FileUtils.deleteQuietly(statsFileCopy);

      memoryForConsumingSegmentPerPartition += getMemoryForInvertedIndex(memoryForConsumingSegmentPerPartition);

      for (int j = 0; j < numHosts.length; j++) {

        int numHostsToProvision = numHosts[j];
        // adjustment because we want ceiling of division and not floor, as some hosts will have an extra partition due to the remainder of the division
        int totalConsumingPartitionsPerHost =
            (totalConsumingPartitions + numHostsToProvision - 1) / numHostsToProvision;

        long totalMemoryForCompletedSegmentsPerHost =
            totalMemoryForCompletedSegmentsPerPartition * totalConsumingPartitionsPerHost;
        long totalMemoryForConsumingSegmentsPerHost =
            memoryForConsumingSegmentPerPartition * totalConsumingPartitionsPerHost;
        long totalMemoryPerHostBytes = totalMemoryForCompletedSegmentsPerHost + totalMemoryForConsumingSegmentsPerHost;

        if (totalMemoryPerHostBytes > MAX_MEMORY_BYTES) {
          _totalMemoryPerHost[i][j] = NOT_APPLICABLE;
          _consumingMemoryPerHost[i][j] = NOT_APPLICABLE;
          _optimalSegmentSize[i][j] = NOT_APPLICABLE;
        } else {
          _totalMemoryPerHost[i][j] = DataSize.fromBytes(totalMemoryPerHostBytes);
          _consumingMemoryPerHost[i][j] = DataSize.fromBytes(totalMemoryForConsumingSegmentsPerHost);
          _optimalSegmentSize[i][j] = DataSize.fromBytes(completedSegmentSizeBytes);
        }
      }
    }
  }

  /**
   * Gets the average num multivalues across all multi value columns in the data
   * @return
   */
  private int getAvgMultiValues() {
    int avgMultiValues = 0;
    Set<String> multiValueColumns = _segmentMetadata.getSchema()
        .getAllFieldSpecs()
        .stream()
        .filter(fieldSpec -> !fieldSpec.isSingleValueField())
        .map(FieldSpec::getName)
        .collect(Collectors.toSet());

    if (!multiValueColumns.isEmpty()) {

      int numValues = 0;
      long multiValuesSum = 0;
      try {
        PinotSegmentRecordReader segmentRecordReader = new PinotSegmentRecordReader(_sampleCompletedSegment);
        GenericRow row = new GenericRow();

        while (segmentRecordReader.hasNext()) {
          segmentRecordReader.next(row);
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
  private long getMemoryForInvertedIndex(long totalMemoryForConsumingSegment) {
    // TODO: better way to estimate inverted indexes memory utilization
    long totalInvertedIndexSizeBytes = 0;
    if (!_invertedIndexColumns.isEmpty()) {
      long memoryForEachColumn = totalMemoryForConsumingSegment / _segmentMetadata.getAllColumns().size();
      totalInvertedIndexSizeBytes = (long) (memoryForEachColumn * 0.3 * _invertedIndexColumns.size());
    }
    return totalInvertedIndexSizeBytes;
  }

  /**
   * Creates a sample realtime segment metadata for the realtime segment config
   * @param segmentMetadata
   * @return
   */
  private RealtimeSegmentZKMetadata getRealtimeSegmentZKMetadata(SegmentMetadataImpl segmentMetadata, int totalDocs) {
    RealtimeSegmentZKMetadata realtimeSegmentZKMetadata = new RealtimeSegmentZKMetadata();
    realtimeSegmentZKMetadata.setStartTime(segmentMetadata.getStartTime());
    realtimeSegmentZKMetadata.setEndTime(segmentMetadata.getEndTime());
    realtimeSegmentZKMetadata.setCreationTime(segmentMetadata.getIndexCreationTime());
    realtimeSegmentZKMetadata.setTableName(segmentMetadata.getTableName());
    realtimeSegmentZKMetadata.setSegmentName(segmentMetadata.getName());
    realtimeSegmentZKMetadata.setTimeUnit(segmentMetadata.getTimeUnit());
    realtimeSegmentZKMetadata.setTotalRawDocs(totalDocs);
    realtimeSegmentZKMetadata.setCrc(Long.parseLong(segmentMetadata.getCrc()));
    return realtimeSegmentZKMetadata;
  }

  /**
   * Given the memory required by a completed segment, this method calculates the total memory required by completed segments at a time for a partition.
   * This calculation takes into account the number of hours the completed segments need to be retained (configured retention - numHoursToConsume)
   * It also takes into account that a new segment will be created every numHoursToConsume hours, and so we might need to keep multiple completed segments in memory at a time
   * @param completedSegmentSizeBytes
   * @param numHoursToConsume
   * @return
   */
  private long calculateMemoryForCompletedSegmentsPerPartition(long completedSegmentSizeBytes, int numHoursToConsume,
      int retentionHours) {

    // if retention is set to x hours, of which we would be consuming for numHoursToConsume hours,
    // the actual number of hours we need to keep completed segment in memory is (x-numHoursToConsume)
    int numHoursToKeepSegmentInMemory = retentionHours - numHoursToConsume;

    // in numHoursToKeepSegmentInMemory hours, a new segment will be created every numHoursToConsume hours
    // hence we need to account for multiple segments which are completed and in memory at the same time
    int numCompletedSegmentsToKeepInMemory =
        (numHoursToKeepSegmentInMemory + numHoursToConsume - 1) / numHoursToConsume; // adjustment for ceil

    return numCompletedSegmentsToKeepInMemory * completedSegmentSizeBytes;
  }

  public String[][] getTotalMemoryPerHost() {
    return _totalMemoryPerHost;
  }

  public String[][] getOptimalSegmentSize() {
    return _optimalSegmentSize;
  }

  public String[][] getConsumingMemoryPerHost() {
    return _consumingMemoryPerHost;
  }
}
