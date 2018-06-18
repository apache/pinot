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
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.PinotSegmentRecordReader;
import com.linkedin.pinot.core.io.readerwriter.RealtimeIndexOffHeapMemoryManager;
import com.linkedin.pinot.core.io.readerwriter.impl.FixedByteSingleColumnMultiValueReaderWriter;
import com.linkedin.pinot.core.io.readerwriter.impl.FixedByteSingleColumnSingleValueReaderWriter;
import com.linkedin.pinot.core.io.writer.impl.DirectMemoryManager;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionaryFactory;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.tools.Command;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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

    long[][] totalMemoryPerHostMB = new long[NUM_HOSTS.length][NUM_HOURS_TO_CONSUME.length];
    long[][] optimalSegmentSizeMB = new long[NUM_HOSTS.length][NUM_HOURS_TO_CONSUME.length];
    File sampleCompletedSegmentFile = new File(_sampleCompletedSegmentDir);
    SegmentMetadataImpl segmentMetadata;
    try {
      segmentMetadata = new SegmentMetadataImpl(sampleCompletedSegmentFile);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception when reading segment index dir", e);
    }
    Map<String, Integer> columnToAvgLength = getAvgStringColumnLengthMap(sampleCompletedSegmentFile, segmentMetadata);
    Map<String, Integer> columnToAvgMultiValues = getAvgMultiValuesMap(sampleCompletedSegmentFile, segmentMetadata);

    int i = 0;
    for (int numHosts : NUM_HOSTS) {
      LOGGER.info("Num hosts : {}", numHosts);

      int j = 0;
      for (int numHoursToConsume : NUM_HOURS_TO_CONSUME) {
        LOGGER.info("Num hours to consume : {}", numHoursToConsume);

        int totalConsumingPartitions = _numPartitions * _numReplicas;
        // adjustment because we want ceiling of division and not floor, as some hosts will have an extra partition due to the remainder of the division
        int totalConsumingPartitionsPerHost = (totalConsumingPartitions + numHosts - 1) / numHosts;

        // calculate completed component
        long sampleCompletedSegmentSizeBytes = FileUtils.sizeOfDirectory(sampleCompletedSegmentFile);
        // consuming for _numHoursSampleSegmentConsumed, and got size sampleCompletedSegmentSizeBytes
        // hence, consuming for numHoursToConsume would give:
        long completedSegmentSizeBytes =
            (long) ((new Double(numHoursToConsume) / _numHoursSampleSegmentConsumed) * sampleCompletedSegmentSizeBytes);

        long totalMemoryForCompletedSegmentsPerHost =
            calculateMemoryForCompletedSegmentsPerHost(completedSegmentSizeBytes, numHoursToConsume,
                totalConsumingPartitionsPerHost);

        // calculate consuming component
        long totalMemoryForConsumingSegmentsPerHost =
            calculateMemoryForConsumingSegmentsPerHost(segmentMetadata, columnToAvgLength, columnToAvgMultiValues,
                numHoursToConsume, totalConsumingPartitionsPerHost);

        // add them up
        long totalMemoryPerHost = totalMemoryForCompletedSegmentsPerHost + totalMemoryForConsumingSegmentsPerHost;

        totalMemoryPerHostMB[i][j] = totalMemoryPerHost;
        optimalSegmentSizeMB[i][j++] = completedSegmentSizeBytes;
      }
      i++;
    }

    System.out.print("    ");
    for (int numHours : NUM_HOURS_TO_CONSUME) {
      System.out.print(numHours);
      System.out.print("      |");
    }
    System.out.println();

    i = 0;
    for (int numHosts : NUM_HOSTS) {
      int j = 0;
      System.out.print(numHosts);
      System.out.print("   ");
      for (int numHours : NUM_HOURS_TO_CONSUME) {
        System.out.print((int)(new Double(totalMemoryPerHostMB[i][j++])/(1024*1024)));
        System.out.print("  |");
      }
      System.out.println();
      i++;
    }

    System.out.println();
    System.out.print("    ");
    for (int numHours : NUM_HOURS_TO_CONSUME) {
      System.out.print(numHours);
      System.out.print("   |");
    }
    System.out.println();

    i = 0;
    for (int numHosts : NUM_HOSTS) {
      int j = 0;
      System.out.print(numHosts);
      System.out.print("   ");
      for (int numHours : NUM_HOURS_TO_CONSUME) {
        System.out.print((int)(new Double(optimalSegmentSizeMB[i][j++])/(1024*1024)));
        System.out.print("  |");
      }
      System.out.println();
      i++;
    }
    return true;
  }

  // TODO: why not do this just once for sample segment, and then derive memory for desired number of rows by direct proportion
  private long calculateMemoryForConsumingSegmentsPerHost(SegmentMetadataImpl segmentMetadata,
      Map<String, Integer> columnToAvgLength, Map<String, Integer> columnToAvgMultiValues, int numHoursToConsume,
      int totalConsumingPartitionsPerHost) throws IOException {

    int totalDocsInSampleSegment = segmentMetadata.getTotalDocs();

    // numHoursSampleSegmentConsumed created totalDocsInSampleSegment num rows
    // numHoursToConsume will create ? rows
    int totalDocs = (int) ((new Double(numHoursToConsume) / _numHoursSampleSegmentConsumed) * totalDocsInSampleSegment);

    long memoryForConsumingSegment = 0;
    for (FieldSpec fieldSpec : segmentMetadata.getSchema().getAllFieldSpecs()) {
      String column = fieldSpec.getName();
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);

      RealtimeIndexOffHeapMemoryManager memoryManager = new DirectMemoryManager(segmentMetadata.getName());

     FieldSpec.DataType dataType = fieldSpec.getDataType();
      int indexColumnSize = FieldSpec.DataType.INT.size();
      if (!columnMetadata.hasDictionary() && fieldSpec.isSingleValueField() && dataType != FieldSpec.DataType.STRING
          && !columnMetadata.hasInvertedIndex()) {
        indexColumnSize = dataType.size();
      } else {
        int dictionaryColumnSize;
        if (dataType == FieldSpec.DataType.STRING) {
          dictionaryColumnSize = columnToAvgLength.get(column);
        } else {
          dictionaryColumnSize = dataType.size();
        }
        String allocationContext =
            buildAllocationContext(segmentMetadata.getName(), column, V1Constants.Dict.FILE_EXTENSION);
        MutableDictionaryFactory.getMutableDictionary(dataType, true, memoryManager, dictionaryColumnSize,
            Math.min(columnMetadata.getCardinality(), totalDocs), allocationContext);
      }

      if (fieldSpec.isSingleValueField()) {
        String allocationContext = buildAllocationContext(segmentMetadata.getName(), column,
            V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION);
        new FixedByteSingleColumnSingleValueReaderWriter(totalDocs, indexColumnSize, memoryManager, allocationContext);
      } else {
        String allocationContext = buildAllocationContext(segmentMetadata.getName(), column,
            V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION);
        new FixedByteSingleColumnMultiValueReaderWriter(columnMetadata.getMaxNumberOfMultiValues(),
            columnToAvgMultiValues.get(column), totalDocs, indexColumnSize, memoryManager, allocationContext);
      }
      memoryForConsumingSegment += memoryManager.getTotalAllocatedBytes();

      if (columnMetadata.hasInvertedIndex()) {
        // account for inv index which would be on heap - add 30% overhead
        memoryForConsumingSegment += new Double(memoryManager.getTotalAllocatedBytes()) * 0.3;
      }
      memoryManager.close();
    }

    long totalMemoryForConsumingSegmentsPerHost = memoryForConsumingSegment * totalConsumingPartitionsPerHost;
    return totalMemoryForConsumingSegmentsPerHost;
  }

  private Map<String, Integer> getAvgStringColumnLengthMap(File sampleCompletedSegmentFile,
      SegmentMetadataImpl segmentMetadata) {
    Map<String, Integer> columnToAvgColumnLengthMap = new HashMap<>();
    Set<String> columns = new HashSet<>();
    for (FieldSpec fieldSpec : segmentMetadata.getSchema().getAllFieldSpecs()) {
      if (fieldSpec.getDataType().equals(FieldSpec.DataType.STRING)) {
        columns.add(fieldSpec.getName());
      }
    }
    if (!columns.isEmpty()) {
      Map<String, Long> columnToColumnLengthSumMap = new HashMap<>(columns.size());
      for (String column : columns) {
        columnToColumnLengthSumMap.put(column, 0L);
      }

      int numValues = 0;
      try {
        PinotSegmentRecordReader segmentRecordReader = new PinotSegmentRecordReader(sampleCompletedSegmentFile);
        GenericRow row = new GenericRow();

        while (segmentRecordReader.hasNext()) {
          segmentRecordReader.next(row);
          for (String column : columns) {
            columnToColumnLengthSumMap.put(column,
                columnToColumnLengthSumMap.get(column) + row.getValue(column).toString().length());
          }
          numValues++;
          row.clear();
        }
      } catch (Exception e) {
        throw new RuntimeException("Caught exception when calculating avg column length");
      }

      for (String column : columns) {
        columnToAvgColumnLengthMap.put(column, (int) (columnToColumnLengthSumMap.get(column) / numValues));
      }
    }
    return columnToAvgColumnLengthMap;
  }

  private Map<String, Integer> getAvgMultiValuesMap(File sampleCompletedSegmentFile,
      SegmentMetadataImpl segmentMetadata) {
    Map<String, Integer> columnToAvgMultiValuesMap = new HashMap<>();

    Set<String> multiValueColumns = new HashSet<>();
    for (FieldSpec fieldSpec : segmentMetadata.getSchema().getAllFieldSpecs()) {
      if (!fieldSpec.isSingleValueField()) {
        multiValueColumns.add(fieldSpec.getName());
      }
    }

    if (!multiValueColumns.isEmpty()) {
      Map<String, Long> columnToNumMultiValuesSumMap = new HashMap<>(multiValueColumns.size());
      for (String multiValueColumn : multiValueColumns) {
        columnToNumMultiValuesSumMap.put(multiValueColumn, 0L);
      }

      int numValues = 0;
      try {
        PinotSegmentRecordReader segmentRecordReader = new PinotSegmentRecordReader(sampleCompletedSegmentFile);
        GenericRow row = new GenericRow();

        while (segmentRecordReader.hasNext()) {
          segmentRecordReader.next(row);
          for (String multiValueColumn : multiValueColumns) {
            int numMultiValues = ((Object[]) (row.getValue(multiValueColumn))).length;
            columnToNumMultiValuesSumMap.put(multiValueColumn,
                columnToNumMultiValuesSumMap.get(multiValueColumn) + numMultiValues);
          }
          numValues++;
          row.clear();
        }
      } catch (Exception e) {
        throw new RuntimeException("Caught exception when calculating avg multi values");
      }

      for (String multiValueColumn : multiValueColumns) {
        columnToAvgMultiValuesMap.put(multiValueColumn,
            (int) (columnToNumMultiValuesSumMap.get(multiValueColumn) / numValues));
      }
    }
    return columnToAvgMultiValuesMap;
  }

  private String buildAllocationContext(String segmentName, String columnName, String indexType) {
    return segmentName + ":" + columnName + indexType;
  }

  private long calculateMemoryForCompletedSegmentsPerHost(long completedSegmentSizeBytes, int numHoursToConsume,
      int totalConsumingPartitionsPerHost) {

    // if retention is set to x hours, of which we would be consuming for numHoursToConsume hours,
    // the actual number of hours we need to keep completed segment in memory is (x-numHoursToConsume)
    int numHoursToKeepSegmentInMemory = _retentionHours - numHoursToConsume;

    // in numHoursToKeepSegmentInMemory hours, a new segment will be created every numHoursToConsume hours
    // hence we need to account for multiple segments which are completed and in memory at the same time
    int numCompletedSegmentsToKeepInMemory =
        (numHoursToKeepSegmentInMemory + numHoursToConsume - 1) / numHoursToConsume; // adjustment for ceil

    long totalMemoryForCompletedSegmentsPerPartition = numCompletedSegmentsToKeepInMemory * completedSegmentSizeBytes;
    long totalMemoryForCompletedSegmentsPerHost =
        totalMemoryForCompletedSegmentsPerPartition * totalConsumingPartitionsPerHost;
    return totalMemoryForCompletedSegmentsPerHost;
  }
}
