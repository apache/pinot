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
package org.apache.pinot.tools.admin.command;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.controller.recommender.io.metadata.SchemaWithMetaData;
import org.apache.pinot.controller.recommender.realtime.provisioning.MemoryEstimator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Given a set of input params, output a table of num hosts to num hours and the memory required per host
 *
 */
@CommandLine.Command(name = "RealtimeProvisioningHelper")
public class RealtimeProvisioningHelperCommand extends AbstractBaseAdminCommand implements Command {

  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeProvisioningHelperCommand.class);

  private static final int MEMORY_STR_LEN = 16;
  private static final String COMMA_SEPARATOR = ",";
  private static final int DEFAULT_RETENTION_FOR_HOURLY_PUSH = 24;
  private static final int DEFAULT_RETENTION_FOR_DAILY_PUSH = 72;
  private static final int DEFAULT_RETENTION_FOR_WEEKLY_PUSH = 24 * 7 + 72;
  private static final int DEFAULT_RETENTION_FOR_MONTHLY_PUSH = 24 * 31 + 72;
  private static final int DEFAULT_NUMBER_OF_ROWS = 10_000;

  @CommandLine.Option(names = {"-tableConfigFile"}, required = true)
  private String _tableConfigFile;

  @CommandLine.Option(names = {"-numPartitions"}, required = true,
      description = "number of stream partitions for the table")
  private int _numPartitions;

  @CommandLine.Option(names = {"-retentionHours"},
      description = "Number of recent hours queried most often" + "\n\t(-pushFrequency is ignored)")
  private int _retentionHours;

  @CommandLine.Option(names = {"-pushFrequency"},
      description = "Frequency with which offline table pushes happen, if this is a hybrid table"
          + "\n\t(hourly,daily,weekly,monthly). Do not specify if realtime-only table")
  private String _pushFrequency;

  @CommandLine.Option(names = {"-numHosts"},
      description = "number of hosts as comma separated values (default 2,4,6,8,10,12,14,16)")
  private String _numHosts = "2,4,6,8,10,12,14,16";

  @CommandLine.Option(names = {"-numHours"},
      description = "number of hours to consume as comma separated values (default 2,3,4,5,6,7,8,9,10,11,12)")
  private String _numHours = "2,3,4,5,6,7,8,9,10,11,12";

  @CommandLine.Option(names = {"-sampleCompletedSegmentDir"}, required = false,
      description = "Consume from the topic for n hours and provide the path of the segment dir after it completes")
  private String _sampleCompletedSegmentDir;

  @CommandLine.Option(names = {"-schemaWithMetadataFile"}, required = false,
      description = "Schema file with extra information on each column describing characteristics of data")
  private String _schemaWithMetadataFile;

  @CommandLine.Option(names = {"-numRows"}, required = false,
      description = "Number of rows to be generated based on schema with metadata file")
  private int _numRows;

  @CommandLine.Option(names = {"-ingestionRate"}, required = true, description = "Avg number of messages per second "
      + "ingested on any one stream partition (assumed all partitions are uniform)")
  private int _ingestionRate;

  @CommandLine.Option(names = {"-maxUsableHostMemory"}, required = false,
      description = "Maximum memory per host that can be used for pinot data (e.g. 250G, 100M). Default 48g")
  private String _maxUsableHostMemory = "48G";

  @CommandLine.Option(names = {"-help", "-h", "--h", "--help"}, usageHelp = true)
  private boolean _help = false;

  public RealtimeProvisioningHelperCommand setTableConfigFile(String tableConfigFile) {
    _tableConfigFile = tableConfigFile;
    return this;
  }

  public RealtimeProvisioningHelperCommand setNumPartitions(int numPartitions) {
    _numPartitions = numPartitions;
    return this;
  }

  public RealtimeProvisioningHelperCommand setPushFrequency(String pushFrequency) {
    _pushFrequency = pushFrequency;
    return this;
  }

  public RealtimeProvisioningHelperCommand setRetentionHours(int retentionHours) {
    _retentionHours = retentionHours;
    return this;
  }

  public RealtimeProvisioningHelperCommand setNumHosts(String numHosts) {
    _numHosts = numHosts;
    return this;
  }

  public RealtimeProvisioningHelperCommand setMaxUsableHostMemory(String maxUsableHostMemory) {
    _maxUsableHostMemory = maxUsableHostMemory;
    return this;
  }

  public RealtimeProvisioningHelperCommand setNumHours(String numHours) {
    _numHours = numHours;
    return this;
  }

  public RealtimeProvisioningHelperCommand setSampleCompletedSegmentDir(String sampleCompletedSegmentDir) {
    _sampleCompletedSegmentDir = sampleCompletedSegmentDir;
    return this;
  }

  public RealtimeProvisioningHelperCommand setIngestionRate(int ingestionRate) {
    _ingestionRate = ingestionRate;
    return this;
  }

  public RealtimeProvisioningHelperCommand setNumRows(int numRows) {
    _numRows = numRows;
    return this;
  }

  @Override
  public String toString() {
    String segmentStr = _sampleCompletedSegmentDir != null ? " -sampleCompletedSegmentDir " + _sampleCompletedSegmentDir
        : " -schemaWithMetadataFile " + _schemaWithMetadataFile + " -numRows " + _numRows;
    return "RealtimeProvisioningHelper -tableConfigFile " + _tableConfigFile + " -numPartitions " + _numPartitions
        + " -pushFrequency " + _pushFrequency + " -numHosts " + _numHosts + " -numHours " + _numHours + segmentStr
        + " -ingestionRate " + _ingestionRate + " -maxUsableHostMemory " + _maxUsableHostMemory + " -retentionHours "
        + _retentionHours;
  }

  @Override
  public final String getName() {
    return "RealtimeProvisioningHelperCommand";
  }

  @Override
  public String description() {
    return "Given the table config, partitions, retention and a sample completed segment for a realtime table to be "
        + "setup, "
        + "this tool will provide memory used by each host and an optimal segment size for various combinations "
        + "of hours to consume and hosts. "
        + "Instead of a completed segment, if schema with characteristics of data is provided, a segment will be "
        + "generated and used for memory estimation.";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public void printExamples() {
    StringBuilder builder = new StringBuilder();
    builder.append("\n\nThis command allows you to estimate the capacity needed for provisioning realtime hosts. ")
        .append("It assumes that there is no upper limit to the amount of memory you can mmap").append(
        "\nIf you have a hybrid table, then consult the push frequency setting in your offline table specify it in "
            + "the -pushFrequency argument").append(
        "\nIf you have a realtime-only table, then the default behavior is to assume that your queries need all "
            + "data in memory all the time").append(
        "\nHowever, if most of your queries are going to be for (say) the last 96 hours, then you can specify "
            + "that in -retentionHours").append(
        "\nDoing so will let this program assume that you are willing to take a page hit when querying older data")
        .append("\nand optimize memory and number of hosts accordingly.")
        .append("\n See https://docs.pinot.apache.org/operators/operating-pinot/tuning/realtime for details");
    System.out.println(builder);
  }

  @Override
  public boolean execute()
      throws IOException {

    boolean segmentProvided = _sampleCompletedSegmentDir != null;
    boolean characteristicsProvided = _schemaWithMetadataFile != null;
    Preconditions.checkState(segmentProvided ^ characteristicsProvided,
        "Either completed segment should be provided or schema with characteristics file!");

    LOGGER.info("Executing command: {}", toString());

    TableConfig tableConfig;
    try (FileInputStream fis = new FileInputStream(new File(_tableConfigFile))) {
      String tableConfigString = IOUtils.toString(fis);
      tableConfig = JsonUtils.stringToObject(tableConfigString, TableConfig.class);
    } catch (IOException e) {
      throw new RuntimeException("Exception in reading table config from file " + _tableConfigFile, e);
    }

    StringBuilder note = new StringBuilder();
    note.append("\nNote:\n");
    int numReplicas = tableConfig.getReplication();
    int tableRetentionHours = (int) TimeUnit.valueOf(tableConfig.getValidationConfig().getRetentionTimeUnit())
        .toHours(Long.parseLong(tableConfig.getValidationConfig().getRetentionTimeValue()));
    if (_retentionHours > 0) {
      note.append(
          "\n* Table retention and push frequency ignored for determining retentionHours since it is specified in "
              + "command");
    } else {
      if (_pushFrequency == null) {
        // This is a realtime-only table. Pick up the retention time
        _retentionHours = tableRetentionHours;
        note.append("\n* Retention hours taken from tableConfig");
      } else {
        if ("hourly".equalsIgnoreCase(_pushFrequency)) {
          _retentionHours = DEFAULT_RETENTION_FOR_HOURLY_PUSH;
        } else if ("daily".equalsIgnoreCase(_pushFrequency)) {
          _retentionHours = DEFAULT_RETENTION_FOR_DAILY_PUSH;
        } else if ("weekly".equalsIgnoreCase(_pushFrequency)) {
          _retentionHours = DEFAULT_RETENTION_FOR_WEEKLY_PUSH;
        } else if ("monthly".equalsIgnoreCase(_pushFrequency)) {
          _retentionHours = DEFAULT_RETENTION_FOR_MONTHLY_PUSH;
        } else {
          throw new IllegalArgumentException("Illegal value for pushFrequency: '" + _pushFrequency + "'");
        }
        note.append("\n* Retention hours taken from pushFrequency");
      }
    }

    int[] numHosts = Arrays.stream(_numHosts.split(COMMA_SEPARATOR)).mapToInt(Integer::parseInt).sorted().toArray();
    int[] numHours = Arrays.stream(_numHours.split(COMMA_SEPARATOR)).mapToInt(Integer::parseInt).sorted().toArray();

    int totalConsumingPartitions = _numPartitions * numReplicas;

    // TODO: allow multiple segments.
    // Consuming: Build statsHistory using multiple segments. Use multiple data points of (totalDocs,
    // numHoursConsumed) to calculate totalDocs for our numHours
    // Completed: Use multiple (completedSize,numHours) data points to calculate completed size for our numHours

    long maxUsableHostMemBytes = DataSizeUtils.toBytes(_maxUsableHostMemory);

    File workingDir = Files.createTempDir();
    File file = new File(_schemaWithMetadataFile);
    Schema schema = deserialize(file, Schema.class);
    MemoryEstimator memoryEstimator;
    if (segmentProvided) {
      // use the provided segment to estimate memory
      memoryEstimator =
          new MemoryEstimator(tableConfig, schema, new File(_sampleCompletedSegmentDir), _ingestionRate,
              maxUsableHostMemBytes, tableRetentionHours, workingDir);
    } else {
      // no segments provided;
      // generate a segment based on the provided characteristics and then use it to estimate memory
      if (_numRows == 0) {
        _numRows = DEFAULT_NUMBER_OF_ROWS;
      }
      SchemaWithMetaData schemaWithMetaData = deserialize(file, SchemaWithMetaData.class);
      memoryEstimator =
          new MemoryEstimator(tableConfig, schema, schemaWithMetaData, _numRows, _ingestionRate, maxUsableHostMemBytes,
              tableRetentionHours, workingDir);
    }
    File sampleStatsHistory = memoryEstimator.initializeStatsHistory();
    memoryEstimator
        .estimateMemoryUsed(sampleStatsHistory, numHosts, numHours, totalConsumingPartitions, _retentionHours);

    note.append("\n* See https://docs.pinot.apache.org/operators/operating-pinot/tuning/realtime");
    // TODO: Make a recommendation of what config to choose by considering more inputs such as qps
    displayOutputHeader(note);
    LOGGER.info("\nMemory used per host (Active/Mapped)");
    displayResults(memoryEstimator.getActiveMemoryPerHost(), numHosts, numHours);
    LOGGER.info("\nOptimal segment size");
    displayResults(memoryEstimator.getOptimalSegmentSize(), numHosts, numHours);
    LOGGER.info("\nConsuming memory");
    displayResults(memoryEstimator.getConsumingMemoryPerHost(), numHosts, numHours);
    LOGGER.info("\nTotal number of segments queried per host (for all partitions)");
    displayResults(memoryEstimator.getNumSegmentsQueriedPerHost(), numHosts, numHours);
    return true;
  }

  private void displayOutputHeader(StringBuilder note) {
    System.out.println("\n============================================================\n" + toString());
    System.out.println(note.toString());
  }

  /**
   * Displays the output values as a grid of numHoursToConsume vs numHostsToProvision
   * @param outputValues
   * @param numHosts
   * @param numHours
   */
  private void displayResults(String[][] outputValues, int[] numHosts, int[] numHours) {
    System.out.println();
    System.out.print("numHosts --> ");
    for (int numHostsToProvision : numHosts) {
      System.out.print(getStringForDisplay(String.valueOf(numHostsToProvision)));
      System.out.print("|");
    }
    System.out.println();

    System.out.println("numHours");

    for (int r = 0; r < outputValues.length; r++) {
      System.out.print(String.format("%2d", numHours[r]));
      System.out.print(" --------> ");
      for (int c = 0; c < outputValues[r].length; c++) {
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

  private <T> T deserialize(File file, Class<T> clazz) {
    try {
      return JsonUtils.fileToObject(file, clazz);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Cannot read schema file '%s' to '%s' object.", file, clazz.getSimpleName()), e);
    }
  }

  public static void main(String[] args)
      throws IOException {
    RealtimeProvisioningHelperCommand rtProvisioningHelper = new RealtimeProvisioningHelperCommand();
    CommandLine cmdLine = new CommandLine(rtProvisioningHelper);
    CommandLine.ParseResult result = cmdLine.parseArgs(args);
    if (result.isUsageHelpRequested() || result.matchedArgs().size() == 0) {
      cmdLine.usage(System.out);
      rtProvisioningHelper.printUsage();
      return;
    }
    rtProvisioningHelper.execute();
  }
}
