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
package com.linkedin.pinot.tools.admin.command;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.utils.DataSize;
import com.linkedin.pinot.common.utils.time.TimeUtils;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.PinotSegmentRecordReader;
import com.linkedin.pinot.core.indexsegment.mutable.MutableSegmentImpl;
import com.linkedin.pinot.core.io.readerwriter.RealtimeIndexOffHeapMemoryManager;
import com.linkedin.pinot.core.io.writer.impl.DirectMemoryManager;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentConfig;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentStatsHistory;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.tools.Command;
import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Given a set of input params output a table of hosts to hours and the memory required per host
 *
 */
public class RealtimeHostsProvisioningCommand extends AbstractBaseAdminCommand implements Command {

  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeHostsProvisioningCommand.class);
  private static final int MEMORY_STR_LEN = 9;
  private static final long MAX_MEMORY_BYTES = DataSize.toBytes("48G");
  private static final String NOT_APPLICABLE = "NA";

  private static final String TMP_DIR = System.getProperty("java.io.tmpdir") + File.separator;
  private static final String STATS_FILE_NAME = "stats.ser";
  private static final String STATS_FILE_COPY_NAME = "stats.copy.ser";

  // TODO: allow override of numHosts and numHours
  private final int[] NUM_HOSTS = {2, 4, 6, 8, 9, 10, 12, 14, 15, 20};
  private final int[] NUM_HOURS_TO_CONSUME = {2, 3, 4, 6, 8, 9, 10, 11, 12};

  // TODO: pick these details from table config, allow override for retention
  // TODO: add numReplicas as a variable in the output?
  @Option(name = "-numReplicas", required = true, metaVar = "<int>", usage = "number of replicas for the table")
  private int _numReplicas;

  @Option(name = "-numPartitions", required = true, metaVar = "<int>", usage = "number of partitions for the table")
  private int _numPartitions;

  @Option(name = "-retentionHours", required = true, metaVar = "<int>", usage = "number of hours we would require this segment to be in memory")
  private int _retentionHours;

  @Option(name = "-numHosts", metaVar = "<String>", usage = "number of hosts as comma separated values")
  private String _numHosts;

  @Option(name = "-numHours", metaVar = "<String>", usage = "number of hours to consume as comma separated values")
  private String _numHours;

  @Option(name = "-sampleCompletedSegmentDir", required = true, metaVar = "<String>", usage = "Consume from the topic for n hours and provide the path of the segment dir after it completes")
  private String _sampleCompletedSegmentDir;

  @Option(name = "-periodSampleSegmentConsumed", required = true, metaVar = "<String>", usage = "Period for which the sample segment was consuming in format 4h, 5h30m, 40m etc")
  private String _periodSampleSegmentConsumed;

  // TODO: write out all the assumptions made in the help and also in the output
  @Option(name = "-help", help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  // TODO: document all the inputs

  public RealtimeHostsProvisioningCommand setNumReplicas(int numReplicas) {
    _numReplicas = numReplicas;
    return this;
  }

  public RealtimeHostsProvisioningCommand setNumPartitions(int numPartitions) {
    _numPartitions = numPartitions;
    return this;
  }

  public RealtimeHostsProvisioningCommand setRetentionHours(int retentionHours) {
    _retentionHours = retentionHours;
    return this;
  }
  public RealtimeHostsProvisioningCommand setNumHosts(String numHosts) {
    _numHosts = numHosts;
    return this;
  }

  public RealtimeHostsProvisioningCommand setNumHours(String numHours) {
    _numHours = numHours;
    return this;
  }


  public RealtimeHostsProvisioningCommand setSampleCompletedSegmentDir(String sampleCompletedSegmentDir) {
    _sampleCompletedSegmentDir = sampleCompletedSegmentDir;
    return this;
  }

  public RealtimeHostsProvisioningCommand setPeriodSampleSegmentConsumed(String periodSampleSegmentConsumed) {
    _periodSampleSegmentConsumed = periodSampleSegmentConsumed;
    return this;
  }

  @Override
  public String toString() {
    return ("RealtimeHostsProvisioningCommand  -numReplicas " + _numReplicas + " -numPartitions " + _numPartitions
        + " -retentionHours " + _retentionHours + " -sampleCompletedSegmentDir " + _sampleCompletedSegmentDir
        + " -periodSampleSegmentConsumed " + _periodSampleSegmentConsumed);
  }

  @Override
  public final String getName() {
    return "RealtimeHostsProvisioningCommand";
  }

  @Override
  public String description() {
    return
        "Given the num replicas, partitions, retention and a sample completed segment for a realtime table to be setup, "
            + "this tool will provide memory used by each host and an optimal segment size for various combinations of hours to consume and hosts";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public boolean execute() throws Exception {
    LOGGER.info("Executing command: {}", toString());
    String[][] totalMemoryPerHost = new String[NUM_HOURS_TO_CONSUME.length][NUM_HOSTS.length];
    String[][] optimalSegmentSize = new String[NUM_HOURS_TO_CONSUME.length][NUM_HOSTS.length];
    String[][] consumingMemoryPerHost = new String[NUM_HOURS_TO_CONSUME.length][NUM_HOSTS.length];

    int totalConsumingPartitions = _numPartitions * _numReplicas;
    long minutesSampleSegmentConsumed =
        TimeUnit.MINUTES.convert(TimeUtils.convertPeriodToMillis(_periodSampleSegmentConsumed), TimeUnit.MILLISECONDS);

    // TODO: allow multiple segments. What would that mean for the memory calculations?
    // Consuming: Build statsHistory using multiple segments. Use multiple data points of (totalDocs,numHoursConsumed) to calculate totalDocs for our numHours
    // Completed: Use multiple (completedSize,numHours) data points to calculate completed size for our numHours
    File sampleCompletedSegmentFile = new File(_sampleCompletedSegmentDir);
    long sampleCompletedSegmentSizeBytes = FileUtils.sizeOfDirectory(sampleCompletedSegmentFile);
    SegmentMetadataImpl segmentMetadata;
    try {
      segmentMetadata = new SegmentMetadataImpl(sampleCompletedSegmentFile);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception when reading segment index dir", e);
    }
    Set<String> noDictionaryColumns = segmentMetadata.getAllColumns()
        .stream()
        .filter(column -> !segmentMetadata.hasDictionary(column))
        .collect(Collectors.toSet());
    Set<String> invertedIndexColumns = segmentMetadata.getAllColumns()
        .stream()
        .filter(column -> segmentMetadata.getColumnMetadataFor(column).hasInvertedIndex())
        .collect(Collectors.toSet());
    int avgMultiValues = getAvgMultiValues(sampleCompletedSegmentFile, segmentMetadata);

    File tableDataDir = new File(TMP_DIR, segmentMetadata.getTableName());
    FileUtils.deleteDirectory(tableDataDir);
    tableDataDir.mkdir();
    File statsFile = new File(tableDataDir, STATS_FILE_NAME);
    RealtimeSegmentStatsHistory initialStatsHistory = RealtimeSegmentStatsHistory.deserialzeFrom(statsFile);

    initializeStatsHistory(sampleCompletedSegmentFile, segmentMetadata, avgMultiValues, noDictionaryColumns,
        invertedIndexColumns, initialStatsHistory);

    int i = 0;
    for (int numHoursToConsume : NUM_HOURS_TO_CONSUME) {
      long minutesToConsume = numHoursToConsume * 60;
      // calculate completed component
      // consuming for _numHoursSampleSegmentConsumed, and got size sampleCompletedSegmentSizeBytes
      // hence, consuming for numHoursToConsume would give:
      long completedSegmentSizeBytes =
          (long) (((double) minutesToConsume / minutesSampleSegmentConsumed) * sampleCompletedSegmentSizeBytes);

      long totalMemoryForCompletedSegmentsPerPartition =
          calculateMemoryForCompletedSegmentsPerPartition(completedSegmentSizeBytes, numHoursToConsume);

      int totalDocsInSampleSegment = segmentMetadata.getTotalDocs();
      // numHoursSampleSegmentConsumed created totalDocsInSampleSegment num rows
      // numHoursToConsume will create ? rows
      int totalDocs = (int) (((double) minutesToConsume / minutesSampleSegmentConsumed) * totalDocsInSampleSegment);
      RealtimeIndexOffHeapMemoryManager memoryManager = new DirectMemoryManager(segmentMetadata.getName());

      File statsFileCopy = new File(tableDataDir, STATS_FILE_COPY_NAME);
      FileUtils.copyFile(statsFile, statsFileCopy);
      RealtimeSegmentStatsHistory statsHistory = RealtimeSegmentStatsHistory.deserialzeFrom(statsFileCopy);

      RealtimeSegmentZKMetadata segmentZKMetadata = getRealtimeSegmentZKMetadata(segmentMetadata, totalDocs);
      RealtimeSegmentConfig.Builder realtimeSegmentConfigBuilder = new RealtimeSegmentConfig.Builder().setSegmentName(segmentMetadata.getName())
          .setStreamName(segmentMetadata.getTableName())
          .setSchema(segmentMetadata.getSchema())
          .setCapacity(totalDocs)
          .setAvgNumMultiValues(avgMultiValues)
          .setNoDictionaryColumns(noDictionaryColumns)
          .setInvertedIndexColumns(invertedIndexColumns)
          .setRealtimeSegmentZKMetadata(segmentZKMetadata)
          .setOffHeap(true)
          .setMemoryManager(memoryManager)
          .setStatsHistory(statsHistory);

      // create mutable segment impl
      MutableSegmentImpl mutableSegmentImpl = new MutableSegmentImpl(realtimeSegmentConfigBuilder.build());
      long memoryForConsumingSegmentPerPartition = memoryManager.getTotalAllocatedBytes();
      mutableSegmentImpl.destroy();
      FileUtils.deleteQuietly(statsFileCopy);

      // TODO: better way to estimate inverted indexes memory utilization
      memoryForConsumingSegmentPerPartition += getMemoryForInvertedIndex(memoryForConsumingSegmentPerPartition, segmentMetadata, invertedIndexColumns);

      int j = 0;
      for (int numHosts : NUM_HOSTS) {

        // adjustment because we want ceiling of division and not floor, as some hosts will have an extra partition due to the remainder of the division
        int totalConsumingPartitionsPerHost = (totalConsumingPartitions + numHosts - 1) / numHosts;

        long totalMemoryForCompletedSegmentsPerHost =
            totalMemoryForCompletedSegmentsPerPartition * totalConsumingPartitionsPerHost;
        long totalMemoryForConsumingSegmentsPerHost =
            memoryForConsumingSegmentPerPartition * totalConsumingPartitionsPerHost;
        long totalMemoryPerHostBytes = totalMemoryForCompletedSegmentsPerHost + totalMemoryForConsumingSegmentsPerHost;

        if (totalMemoryPerHostBytes > MAX_MEMORY_BYTES) {
          totalMemoryPerHost[i][j] = NOT_APPLICABLE;
          consumingMemoryPerHost[i][j] = NOT_APPLICABLE;
          optimalSegmentSize[i][j++] = NOT_APPLICABLE;
        } else {
          totalMemoryPerHost[i][j] = DataSize.fromBytes(totalMemoryPerHostBytes);
          consumingMemoryPerHost[i][j] = DataSize.fromBytes(totalMemoryForConsumingSegmentsPerHost);
          optimalSegmentSize[i][j++] = DataSize.fromBytes(completedSegmentSizeBytes);
        }
      }
      i++;
    }

    // TODO: Add caveats, that we could be off by x% - what is this x?
    // TODO: Make a recommendation of what config to choose
    System.out.println("\nMemory used per host");
    display(totalMemoryPerHost);
    System.out.println("\nOptimal segment size");
    display(optimalSegmentSize);
    System.out.println("\nConsuming memory");
    display(consumingMemoryPerHost);
    return true;
  }

  /**
   * Computes the memory by the inverted indexes in the consuming segment
   * This is just an estimation. We use MutableRoaringBitmap for inverted indexes, which use heap memory.
   * @param totalMemoryForConsumingSegment
   * @param segmentMetadata
   * @param invertedIndexColumns
   * @return
   */
  private long getMemoryForInvertedIndex(long totalMemoryForConsumingSegment, SegmentMetadataImpl segmentMetadata, Set<String> invertedIndexColumns) {
    long totalInvertedIndexSizeBytes = 0;
    if (!invertedIndexColumns.isEmpty()) {
      long memoryForEachColumn = totalMemoryForConsumingSegment / segmentMetadata.getAllColumns().size();
      totalInvertedIndexSizeBytes = (long) (memoryForEachColumn * 0.3 * invertedIndexColumns.size());
    }
    return totalInvertedIndexSizeBytes;
  }

  private void initializeStatsHistory(File sampleCompletedSegmentFile, SegmentMetadataImpl segmentMetadata,
      int avgMultiValues, Set<String> noDictionaryColumns, Set<String> invertedIndexColumns,
      RealtimeSegmentStatsHistory statsHistory) {

    RealtimeIndexOffHeapMemoryManager memoryManager = new DirectMemoryManager(segmentMetadata.getName());
    RealtimeSegmentZKMetadata segmentZKMetadata = getRealtimeSegmentZKMetadata(segmentMetadata);

    // create a config
    RealtimeSegmentConfig.Builder realtimeSegmentConfigBuilder =
        new RealtimeSegmentConfig.Builder().setSegmentName(segmentMetadata.getName())
            .setStreamName(segmentMetadata.getTableName())
            .setSchema(segmentMetadata.getSchema())
            .setCapacity(segmentMetadata.getTotalDocs())
            .setAvgNumMultiValues(avgMultiValues)
            .setNoDictionaryColumns(noDictionaryColumns)
            .setInvertedIndexColumns(invertedIndexColumns)
            .setRealtimeSegmentZKMetadata(segmentZKMetadata)
            .setOffHeap(true)
            .setMemoryManager(memoryManager)
            .setStatsHistory(statsHistory);

    // create mutable segment impl
    MutableSegmentImpl mutableSegmentImpl = new MutableSegmentImpl(realtimeSegmentConfigBuilder.build());

    // read all rows and index them to populate stats.ser
    try (PinotSegmentRecordReader segmentRecordReader = new PinotSegmentRecordReader(sampleCompletedSegmentFile);) {
      GenericRow row = new GenericRow();
      while (segmentRecordReader.hasNext()) {
        segmentRecordReader.next(row);
        mutableSegmentImpl.index(row);
        row.clear();
      }
    } catch (Exception e) {
      throw new RuntimeException("Caught exception when indexing rows");
    }
    mutableSegmentImpl.destroy();
  }

  /**
   * Creates a sample realtime segment metadata for the realtime segment config
   * @param segmentMetadata
   * @return
   */
  private RealtimeSegmentZKMetadata getRealtimeSegmentZKMetadata(SegmentMetadataImpl segmentMetadata) {
    RealtimeSegmentZKMetadata realtimeSegmentZKMetadata = new RealtimeSegmentZKMetadata();
    realtimeSegmentZKMetadata.setStartTime(segmentMetadata.getStartTime());
    realtimeSegmentZKMetadata.setEndTime(segmentMetadata.getEndTime());
    realtimeSegmentZKMetadata.setCreationTime(segmentMetadata.getIndexCreationTime());
    realtimeSegmentZKMetadata.setTableName(segmentMetadata.getTableName());
    realtimeSegmentZKMetadata.setSegmentName(segmentMetadata.getName());
    realtimeSegmentZKMetadata.setTimeUnit(segmentMetadata.getTimeUnit());
    realtimeSegmentZKMetadata.setTotalRawDocs(segmentMetadata.getTotalRawDocs());
    realtimeSegmentZKMetadata.setCrc(Long.parseLong(segmentMetadata.getCrc()));
    return realtimeSegmentZKMetadata;
  }

  private RealtimeSegmentZKMetadata getRealtimeSegmentZKMetadata(SegmentMetadataImpl segmentMetadata, int totalDocs) {
    RealtimeSegmentZKMetadata realtimeSegmentZKMetadata = getRealtimeSegmentZKMetadata(segmentMetadata);
    realtimeSegmentZKMetadata.setTotalRawDocs(totalDocs);
    return realtimeSegmentZKMetadata;
  }

  /**
   * Gets the average num multivalues across all multivalue columns in the data
   * @param sampleCompletedSegmentFile
   * @param segmentMetadata
   * @return
   */
  private int getAvgMultiValues(File sampleCompletedSegmentFile, SegmentMetadataImpl segmentMetadata) {
    int avgMultiValues = 0;
    Set<String> multiValueColumns = new HashSet<>();
    for (FieldSpec fieldSpec : segmentMetadata.getSchema().getAllFieldSpecs()) {
      if (!fieldSpec.isSingleValueField()) {
        multiValueColumns.add(fieldSpec.getName());
      }
    }

    if (!multiValueColumns.isEmpty()) {

      int numValues = 0;
      long multiValuesSum = 0;
      try {
        PinotSegmentRecordReader segmentRecordReader = new PinotSegmentRecordReader(sampleCompletedSegmentFile);
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
   * Given the memory required by a completed segment, this method calculates the total memory required by completed segments at a time for a partition.
   * This calculation takes into account the number of hours the completed segments need to be retained (configured retention - numHoursToConsume)
   * It also takes into account that a new segment will be created every numHoursToConsume hours, and so we might need to keep multiple completed segments in memory at a time
   * @param completedSegmentSizeBytes
   * @param numHoursToConsume
   * @return
   */
  private long calculateMemoryForCompletedSegmentsPerPartition(long completedSegmentSizeBytes, int numHoursToConsume) {

    // if retention is set to x hours, of which we would be consuming for numHoursToConsume hours,
    // the actual number of hours we need to keep completed segment in memory is (x-numHoursToConsume)
    int numHoursToKeepSegmentInMemory = _retentionHours - numHoursToConsume;

    // in numHoursToKeepSegmentInMemory hours, a new segment will be created every numHoursToConsume hours
    // hence we need to account for multiple segments which are completed and in memory at the same time
    int numCompletedSegmentsToKeepInMemory =
        (numHoursToKeepSegmentInMemory + numHoursToConsume - 1) / numHoursToConsume; // adjustment for ceil

    return numCompletedSegmentsToKeepInMemory * completedSegmentSizeBytes;
  }

  /*******************************************************************
   *
   * Methods for displaying the results
   * TODO: write better methods, or put these in a separate class
   *
   *****************************************************************/

  private void display(String[][] data) {
    printNumHostsHeader();
    printNumHoursHeader();
    printOutputValues(data);
  }

  private void display(String[][] totalMemoryPerHost, String[][] optimalSegmentSize) {

    System.out.println("\nMemory used per host");
    printNumHostsHeader();
    printNumHoursHeader();
    printOutputValues(totalMemoryPerHost);

    System.out.println("\nOptimal segment size");
    printNumHostsHeader();
    printNumHoursHeader();
    printOutputValues(optimalSegmentSize);
  }

  private void printNumHostsHeader() {
    System.out.println();
    System.out.print("numHosts --> ");
    for (int numHosts : NUM_HOSTS) {
      System.out.print(getStringForDisplay(String.valueOf(numHosts)));
      System.out.print("|");
    }
    System.out.println();
  }

  private void printNumHoursHeader() {
    System.out.println("numHours");
  }

  private void printOutputValues(String[][] outputValues) {
    for (int r = 0; r < NUM_HOURS_TO_CONSUME.length; r++) {
      System.out.print(String.format("%2d", NUM_HOURS_TO_CONSUME[r]));
      System.out.print(" --------> ");
      for (int c = 0; c < NUM_HOSTS.length; c++) {
        System.out.print(getStringForDisplay(outputValues[r][c]));
        System.out.print("|");
      }
      System.out.println();
    }
  }

  private String getStringForDisplay(String memoryStr) {
    int numSpacesToPad = MEMORY_STR_LEN - memoryStr.length();
    return memoryStr + StringUtils.repeat(" ", numSpacesToPad);
  }
}
