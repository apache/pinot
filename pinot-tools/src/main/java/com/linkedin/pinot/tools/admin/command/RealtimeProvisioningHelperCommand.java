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
package com.linkedin.pinot.tools.admin.command;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.utils.time.TimeUtils;
import com.linkedin.pinot.tools.Command;
import com.linkedin.pinot.tools.realtime.provisioning.MemoryEstimator;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Given a set of input params, output a table of num hosts to num hours and the memory required per host
 *
 */
public class RealtimeProvisioningHelperCommand extends AbstractBaseAdminCommand implements Command {

  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeProvisioningHelperCommand.class);

  private static final int MEMORY_STR_LEN = 9;
  private static final String COMMA_SEPARATOR = ",";
  private static final int DEFAULT_RETENTION_FOR_HOURLY_PUSH = 24;
  private static final int DEFAULT_RETENTION_FOR_DAILY_PUSH = 72;

  @Option(name = "-tableConfigFile", required = true, metaVar = "<String>")
  private String _tableConfigFile;

  @Option(name = "-numPartitions", required = true, metaVar = "<int>", usage = "number of stream partitions for the table")
  private int _numPartitions;

  @Option(name = "-retentionHours", metaVar = "<int>", usage = "Number of hours the segments will need to be retained in memory. "
      + "\nThe realtime segments will need to be in memory only until the offline segments are available and used for queries"
      + "\nThis will be picked from the table config  by looking at the segmentPushFrequency (72h if daily, 24h if hourly, buffer added as TimeBoundaryService doesn't query the last offline timestamp), "
      + "\nIt can be overridden using this option")
  private int _retentionHours;

  @Option(name = "-numHosts", metaVar = "<String>", usage = "number of hosts as comma separated values (default 2,4,6,8,10,12,14,16)")
  private String _numHosts = "2,4,6,8,10,12,14,16";

  @Option(name = "-numHours", metaVar = "<String>", usage = "number of hours to consume as comma separated values (default 2,3,4,5,6,7,8,9,10,11,12)")
  private String _numHours = "2,3,4,5,6,7,8,9,10,11,12";

  @Option(name = "-sampleCompletedSegmentDir", required = true, metaVar = "<String>", usage = "Consume from the topic for n hours and provide the path of the segment dir after it completes")
  private String _sampleCompletedSegmentDir;

  @Option(name = "-periodSampleSegmentConsumed", required = true, metaVar = "<String>", usage = "Period for which the sample segment was consuming in format 4h, 5h30m, 40m etc")
  private String _periodSampleSegmentConsumed;

  @Option(name = "-help", help = true, aliases = {"-h", "--h", "--help"})
  private boolean _help = false;

  public RealtimeProvisioningHelperCommand setTableConfigFile(String tableConfigFile) {
    _tableConfigFile = tableConfigFile;
    return this;
  }

  public RealtimeProvisioningHelperCommand setNumPartitions(int numPartitions) {
    _numPartitions = numPartitions;
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

  public RealtimeProvisioningHelperCommand setNumHours(String numHours) {
    _numHours = numHours;
    return this;
  }

  public RealtimeProvisioningHelperCommand setSampleCompletedSegmentDir(String sampleCompletedSegmentDir) {
    _sampleCompletedSegmentDir = sampleCompletedSegmentDir;
    return this;
  }

  public RealtimeProvisioningHelperCommand setPeriodSampleSegmentConsumed(String periodSampleSegmentConsumed) {
    _periodSampleSegmentConsumed = periodSampleSegmentConsumed;
    return this;
  }

  @Override
  public String toString() {
    return ("RealtimeProvisioningHelperCommand -tableConfigFile " + _tableConfigFile + " -numPartitions "
        + _numPartitions + " -retentionHours " + _retentionHours + " -numHosts " + _numHosts + " -numHours " + _numHours
        + " -sampleCompletedSegmentDir " + _sampleCompletedSegmentDir + " -periodSampleSegmentConsumed "
        + _periodSampleSegmentConsumed);
  }

  @Override
  public final String getName() {
    return "RealtimeProvisioningHelperCommand";
  }

  @Override
  public String description() {
    return
        "Given the table config, partitions, retention and a sample completed segment for a realtime table to be setup, "
            + "this tool will provide memory used by each host and an optimal segment size for various combinations of hours to consume and hosts";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public boolean execute() throws IOException {
    LOGGER.info("Executing command: {}", toString());

    TableConfig tableConfig;
    try (FileInputStream fis = new FileInputStream(new File(_tableConfigFile))) {
      String tableConfigString = IOUtils.toString(fis);
      tableConfig = TableConfig.fromJSONConfig(new JSONObject(tableConfigString));
    } catch (IOException e) {
      throw new RuntimeException("Exception in reading table config from file " + _tableConfigFile, e);
    }

    int numReplicas = tableConfig.getValidationConfig().getReplicasPerPartitionNumber();
    if (_retentionHours == 0) {
      if (tableConfig.getValidationConfig().getSegmentPushFrequency().equalsIgnoreCase("hourly")) {
        _retentionHours = DEFAULT_RETENTION_FOR_HOURLY_PUSH;
      } else {
        _retentionHours = DEFAULT_RETENTION_FOR_DAILY_PUSH;
      }
    }

    int[] numHosts = Arrays.stream(_numHosts.split(COMMA_SEPARATOR)).mapToInt(Integer::parseInt).toArray();
    int[] numHours = Arrays.stream(_numHours.split(COMMA_SEPARATOR)).mapToInt(Integer::parseInt).toArray();

    int totalConsumingPartitions = _numPartitions * numReplicas;

    // TODO: allow multiple segments.
    // Consuming: Build statsHistory using multiple segments. Use multiple data points of (totalDocs,numHoursConsumed) to calculate totalDocs for our numHours
    // Completed: Use multiple (completedSize,numHours) data points to calculate completed size for our numHours
    File sampleCompletedSegmentFile = new File(_sampleCompletedSegmentDir);

    long sampleSegmentConsumedSeconds =
        TimeUnit.SECONDS.convert(TimeUtils.convertPeriodToMillis(_periodSampleSegmentConsumed), TimeUnit.MILLISECONDS);

    MemoryEstimator memoryEstimator =
        new MemoryEstimator(tableConfig, sampleCompletedSegmentFile, sampleSegmentConsumedSeconds);
    File sampleStatsHistory = memoryEstimator.initializeStatsHistory();
    memoryEstimator.estimateMemoryUsed(sampleStatsHistory, numHosts, numHours, totalConsumingPartitions,
        _retentionHours);

    // TODO: Make a recommendation of what config to choose by considering more inputs such as qps
    LOGGER.info("\nMemory used per host");
    displayResults(memoryEstimator.getTotalMemoryPerHost(), numHosts, numHours);
    LOGGER.info("\nOptimal segment size");
    displayResults(memoryEstimator.getOptimalSegmentSize(), numHosts, numHours);
    LOGGER.info("\nConsuming memory");
    displayResults(memoryEstimator.getConsumingMemoryPerHost(), numHosts, numHours);
    return true;
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
}
