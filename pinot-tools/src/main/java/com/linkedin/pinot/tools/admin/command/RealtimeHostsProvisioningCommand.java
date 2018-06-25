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
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Given a set of input params output a table of hosts to hours and the memory required per host
 *
 */
public class RealtimeHostsProvisioningCommand extends AbstractBaseAdminCommand implements Command {

  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeHostsProvisioningCommand.class);
  private static final String TMP_DIR = System.getProperty("java.io.tmpdir") + File.separator;
  private static final String STATS_FILE_NAME = "stats.ser";

  private final int[] NUM_HOSTS = {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
  private final int[] NUM_HOURS_TO_CONSUME = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};

  @Option(name = "-numReplicas", required = true, metaVar = "<int>", usage = "number of replicas for the table")
  private int _numReplicas;

  @Option(name = "-numPartitions", required = true, metaVar = "<int>", usage = "number of partitions for the table")
  private int _numPartitions;

  @Option(name = "-retentionHours", required = true, metaVar = "<int>", usage = "number of hours we would require this segment to be in memory")
  private int _retentionHours;

  @Option(name = "-sampleCompletedSegmentDir", required = true, metaVar = "<String>", usage = "Consume from the topic for n hours and provide the path of the segment dir after it completes")
  private String _sampleCompletedSegmentDir;

  @Option(name = "-numHoursSampleSegmentConsumed", required = true, metaVar = "<int>", usage = "Number of hours for which the sample segment was consuming")
  private int _numHoursSampleSegmentConsumed;

  @Option(name = "-help", help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

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

  public RealtimeHostsProvisioningCommand setSampleCompletedSegmentDir(String sampleCompletedSegmentDir) {
    _sampleCompletedSegmentDir = sampleCompletedSegmentDir;
    return this;
  }

  public RealtimeHostsProvisioningCommand setNumHoursSampleSegmentConsumed(int numHoursSampleSegmentConsumed) {
    _numHoursSampleSegmentConsumed = numHoursSampleSegmentConsumed;
    return this;
  }

  @Override
  public String toString() {
    return ("RealtimeHostsProvisioningCommand  -numReplicas " + _numReplicas + " -numPartitions " + _numPartitions
        + " -retentionHours " + _retentionHours + " -sampleCompletedSegmentDir " + _sampleCompletedSegmentDir
        + " -numHoursSampleSegmentConsumed " + _numHoursSampleSegmentConsumed);
  }

  @Override
  public final String getName() {
    return "RealtimeHostsProvisioningCommand";
  }

  @Override
  public String description() {
    return
        "Given the num replicas, partitions, retention and a sample completed segment for a realtime table to be setup, "
            + "this tool will provide memory used each host and an optimal segment size for various combinations of hours to consume and hosts";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public boolean execute() throws Exception {
    LOGGER.info("Executing command: {}", toString());
    long[][] totalMemoryPerHostMB = new long[NUM_HOURS_TO_CONSUME.length][NUM_HOSTS.length];
    long[][] optimalSegmentSizeMB = new long[NUM_HOURS_TO_CONSUME.length][NUM_HOSTS.length];

    int totalConsumingPartitions = _numPartitions * _numReplicas;
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
    RealtimeIndexOffHeapMemoryManager memoryManager = new DirectMemoryManager(segmentMetadata.getName());
    RealtimeSegmentZKMetadata segmentZKMetadata = getRealtimeSegmentZKMetadata(segmentMetadata);

    File tableDataDir = new File(TMP_DIR, segmentMetadata.getTableName());
    FileUtils.deleteDirectory(tableDataDir);
    tableDataDir.mkdir();
    File statsFile = new File(tableDataDir, STATS_FILE_NAME);
    RealtimeSegmentStatsHistory statsHistory = RealtimeSegmentStatsHistory.deserialzeFrom(statsFile);

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

    // read all rows and index them
    try {
      PinotSegmentRecordReader segmentRecordReader = new PinotSegmentRecordReader(sampleCompletedSegmentFile);
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

    int i = 0;
    for (int numHoursToConsume : NUM_HOURS_TO_CONSUME) {

      // calculate completed component
      // consuming for _numHoursSampleSegmentConsumed, and got size sampleCompletedSegmentSizeBytes
      // hence, consuming for numHoursToConsume would give:
      long completedSegmentSizeBytes =
          (long) (((double) numHoursToConsume / _numHoursSampleSegmentConsumed) * sampleCompletedSegmentSizeBytes);
      long totalMemoryForCompletedSegmentsPerPartition =
          calculateMemoryForCompletedSegmentsPerPartition(completedSegmentSizeBytes, numHoursToConsume);

      int totalDocsInSampleSegment = segmentMetadata.getTotalDocs();
      // numHoursSampleSegmentConsumed created totalDocsInSampleSegment num rows
      // numHoursToConsume will create ? rows
      int totalDocs = (int) (((double) numHoursToConsume / _numHoursSampleSegmentConsumed) * totalDocsInSampleSegment);
      memoryManager = new DirectMemoryManager(segmentMetadata.getName());
      realtimeSegmentConfigBuilder = new RealtimeSegmentConfig.Builder().setSegmentName(segmentMetadata.getName())
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
      mutableSegmentImpl = new MutableSegmentImpl(realtimeSegmentConfigBuilder.build());
      long memoryForConsumingSegmentPerPartition = memoryManager.getTotalAllocatedBytes();
      mutableSegmentImpl.destroy();

      int j = 0;
      for (int numHosts : NUM_HOSTS) {
        // adjustment because we want ceiling of division and not floor, as some hosts will have an extra partition due to the remainder of the division
        int totalConsumingPartitionsPerHost = (totalConsumingPartitions + numHosts - 1) / numHosts;

        long totalMemoryForCompletedSegmentsPerHost =
            totalMemoryForCompletedSegmentsPerPartition * totalConsumingPartitionsPerHost;
        long totalMemoryForConsumingSegmentsPerHost =
            memoryForConsumingSegmentPerPartition * totalConsumingPartitionsPerHost;
        long totalMemoryPerHost = totalMemoryForCompletedSegmentsPerHost + totalMemoryForConsumingSegmentsPerHost;

        totalMemoryPerHostMB[i][j] = (long) ((double) totalMemoryPerHost / (1024 * 1024));
        optimalSegmentSizeMB[i][j++] = (long) ((double) completedSegmentSizeBytes / (1024 * 1024));
      }
      i++;
    }
    display(totalMemoryPerHostMB, optimalSegmentSizeMB);
    return true;
  }

  private void display(long[][] totalMemoryPerHostMB, long[][] optimalSegmentSizeMB) {
    System.out.print("numHosts --> ");
    for (int numHosts : NUM_HOSTS) {
      System.out.print(numHosts);
      System.out.print("      |");
    }
    System.out.println();

    for (int r = 0; r < NUM_HOURS_TO_CONSUME.length; r++) {
      System.out.print(NUM_HOURS_TO_CONSUME[r]);
      System.out.print(" --->       ");
      for (int c = 0; c < NUM_HOSTS.length; c++) {
        System.out.print(totalMemoryPerHostMB[r][c]);
        System.out.print("  |");
      }
      System.out.println();
    }

    System.out.println();

    System.out.print("numHosts --> ");
    for (int numHosts : NUM_HOSTS) {
      System.out.print(numHosts);
      System.out.print("   |");
    }
    System.out.println();

    for (int r = 0; r < NUM_HOURS_TO_CONSUME.length; r++) {
      System.out.print(NUM_HOURS_TO_CONSUME[r]);
      System.out.print(" -->       ");
      for (int c = 0; c < NUM_HOSTS.length; c++) {
        System.out.print(optimalSegmentSizeMB[r][c]);
        System.out.print("  |");
      }
      System.out.println();
    }
  }

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
      avgMultiValues = (int) ((double) multiValuesSum / numValues);
    }
    return avgMultiValues;
  }

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
}
