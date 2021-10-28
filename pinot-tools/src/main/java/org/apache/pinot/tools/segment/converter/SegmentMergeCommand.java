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
package org.apache.pinot.tools.segment.converter;

import com.google.common.base.Preconditions;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.core.minion.rollup.MergeRollupSegmentConverter;
import org.apache.pinot.core.minion.rollup.MergeType;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.creator.name.NormalizedDateSegmentNameGenerator;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.admin.command.AbstractBaseAdminCommand;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Segment merge command.
 *
 * TODO: add support for m to n merge, add support for rollup type
 */
public class SegmentMergeCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentMergeCommand.class);

  private static final String INPUT_SEGMENT_SEPARATOR = ",";
  private static final String DEFAULT_MERGE_TYPE = "CONCATENATE";
  private static final int DEFAULT_SEQUENCE_ID = 0;

  @Option(name = "-inputPaths", required = true, metaVar = "<String>",
      usage = "Comma separated input segment files or directories that contains input segments to be merged")
  private String _inputSegmentPaths;

  @Option(name = "-outputPath", required = true, metaVar = "<String>",
      usage = "Output segment path. This should be different from working directory.")
  private String _outputPath;

  @Option(name = "-tableConfigFilePath", required = true, metaVar = "<String>", usage = "Table config file path.")
  private String _tableConfigFilePath;

  @Option(name = "-schemaFilePath", required = true, metaVar = "<String>", usage = "Schema file path")
  private String _schemaFilePath;

  @Option(name = "-tarOutputSegment", required = true, metaVar = "<String>",
      usage = "Indicate whether to tar output segment (true, false)")
  private String _tarOutputSegment;

  @Option(name = "-outputSegmentName", required = false, metaVar = "<String>",
      usage = "The name of output segment file")
  private String _outputSegmentName;

  // TODO: once rollup mode is supported, make this field required
  @Option(name = "-mergeType", required = false, metaVar = "<String>",
      usage = "Merge type (\"CONCATENATE\" or \"ROLLUP\"). Currently, only \"CONCATENATE\" type is supported.")
  private String _mergeType = DEFAULT_MERGE_TYPE;

  @Option(name = "-workingDirectory", required = false, metaVar = "<String>",
      usage = "Path for working directory. This directory gets cleaned up after the job")
  private String _workingDirectory;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"},
      usage = "Print this message.")
  private boolean _help = false;

  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "SegmentConcatenation";
  }

  @Override
  public boolean execute()
      throws Exception {
    LOGGER.info("Running segment merge command...");

    // Check merge type
    if (MergeType.valueOf(_mergeType) != MergeType.CONCATENATE) {
      throw new InvalidConfigException("Currently, only CONCATENATE merge type is supported");
    }

    // Set working directory
    File workingDir;
    if (_workingDirectory == null) {
      workingDir = FileUtils.getTempDirectory();
    } else {
      workingDir = new File(_workingDirectory);
    }

    // Check if the input path is directory or comma separated files
    String[] paths = _inputSegmentPaths.split(INPUT_SEGMENT_SEPARATOR);
    List<String> inputPaths = new ArrayList<>();
    for (String path : paths) {
      addFilePath(inputPaths, path.trim());
    }
    Preconditions.checkState(inputPaths.size() > 1, "Input paths has to contain at least 2 segments");
    LOGGER.info("Input segments: " + inputPaths);

    try {
      // Get the list of input segment index directories
      List<File> inputIndexDirs = new ArrayList<>();
      File untarredSegments = new File(workingDir, "untarredSegments");
      Preconditions.checkState(untarredSegments.mkdirs());
      int segmentNum = 0;
      for (String segmentPath : inputPaths) {
        File segmentFile = new File(segmentPath);
        if (segmentFile.isDirectory() && isPinotSegment(segmentFile)) {
          inputIndexDirs.add(segmentFile);
        } else {
          File segmentDir = new File(untarredSegments, "segmentDir_" + segmentNum++);
          File indexDir = TarGzCompressionUtils.untar(segmentFile, segmentDir).get(0);
          inputIndexDirs.add(indexDir);
        }
      }
      LOGGER.info("Input segment paths for segment generator: {}", inputIndexDirs);

      // Read table config
      String tableConfigString =
          new String(Files.readAllBytes(Paths.get(_tableConfigFilePath)), StandardCharsets.UTF_8);
      TableConfig tableConfig = JsonUtils.stringToObject(tableConfigString, TableConfig.class);

      // Read schema
      Schema schema = Schema.fromFile(new File(_schemaFilePath));

      LOGGER.info("Table config: {}", tableConfig);
      LOGGER.info("Schema : {}", schema);

      // Compute mix/max time from segment metadata
      long minStartTime = Long.MAX_VALUE;
      long maxEndTime = Long.MIN_VALUE;
      long totalNumDocsBeforeMerge = 0L;
      Iterator<File> it = inputIndexDirs.iterator();
      while (it.hasNext()) {
        File indexDir = it.next();
        SegmentMetadata segmentMetadata = new SegmentMetadataImpl(indexDir);
        if (segmentMetadata.getTotalDocs() > 0) {
          long currentStartTime = segmentMetadata.getStartTime();
          if (currentStartTime < minStartTime) {
            minStartTime = currentStartTime;
          }

          long currentEndTime = segmentMetadata.getEndTime();
          if (currentEndTime > maxEndTime) {
            maxEndTime = currentEndTime;
          }
          totalNumDocsBeforeMerge += segmentMetadata.getTotalDocs();
        } else {
          LOGGER.info("Discarding segment {} since it has 0 records", segmentMetadata.getName());
          it.remove();
        }
      }

      // Compute segment name if it is not specified
      if (_outputSegmentName == null) {
        _outputSegmentName = getDefaultSegmentName(tableConfig, schema, minStartTime, maxEndTime);
      }
      LOGGER.info("Output segment name: {}", _outputSegmentName);

      // Compute segment merge
      // TODO: add support for rollup
      String tableName = TableNameBuilder.extractRawTableName(tableConfig.getTableName());
      MergeRollupSegmentConverter mergeRollupSegmentConverter =
          new MergeRollupSegmentConverter.Builder().setMergeType(MergeType.fromString(_mergeType))
              .setSegmentName(_outputSegmentName).setInputIndexDirs(inputIndexDirs).setWorkingDir(workingDir)
              .setTableName(tableName).setTableConfig(tableConfig).build();

      List<File> outputSegments = mergeRollupSegmentConverter.convert();
      Preconditions.checkState(outputSegments.size() == 1);
      File outputSegment = outputSegments.get(0);

      // Make sure to create output directory
      File outputDir = new File(_outputPath);
      if (!outputDir.exists()) {
        Preconditions.checkState(outputDir.mkdirs());
      }

      // Get segment name from segment metadata
      SegmentMetadata outputSegmentMetadata = new SegmentMetadataImpl(outputSegment);
      String outputSegmentName = outputSegmentMetadata.getName();

      // Move the merged segment to output directory.
      File finalOutputPath = new File(outputDir, outputSegmentName);
      if (Boolean.parseBoolean(_tarOutputSegment)) {
        File segmentTarFile = new File(workingDir, outputSegmentName + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
        TarGzCompressionUtils.createTarGzFile(outputSegment, segmentTarFile);
        FileUtils.moveFile(segmentTarFile, finalOutputPath);
      } else {
        FileUtils.moveDirectory(outputSegment, finalOutputPath);
      }
      LOGGER.info("Segment has been merged correctly. Output file is located at {}", finalOutputPath);
      LOGGER.info("Min start time / max end time for input segments : " + minStartTime + " / " + maxEndTime);
      LOGGER.info("Min start time / max end time for merged segment: " + outputSegmentMetadata.getStartTime() + " / "
          + outputSegmentMetadata.getEndTime());
      LOGGER.info("Total number of documents for input segments: " + totalNumDocsBeforeMerge);
      LOGGER.info("Total number of documents for merged segment: " + outputSegmentMetadata.getTotalDocs());
    } finally {
      // Clean up working directory
      FileUtils.deleteQuietly(workingDir);
    }
    return true;
  }

  @Override
  public String description() {
    return "Create the merged segment using concatenation";
  }

  private String getDefaultSegmentName(TableConfig tableConfig, Schema schema, long minStartTime, long maxEndTime) {
    String tableName = tableConfig.getTableName();

    // Fetch time related configurations from schema and table config.
    SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();
    String pushFrequency = IngestionConfigUtils.getBatchSegmentIngestionFrequency(tableConfig);
    String pushType = IngestionConfigUtils.getBatchSegmentIngestionType(tableConfig);
    String timeColumnName = validationConfig.getTimeColumnName();
    DateTimeFormatSpec dateTimeFormatSpec = null;
    if (timeColumnName != null) {
      DateTimeFieldSpec dateTimeSpec = schema.getSpecForTimeColumn(timeColumnName);
      if (dateTimeSpec != null) {
        dateTimeFormatSpec = new DateTimeFormatSpec(dateTimeSpec.getFormat());
      }
    }

    // Generate the final segment name using segment name generator
    NormalizedDateSegmentNameGenerator segmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(tableName, null, false, pushType, pushFrequency, dateTimeFormatSpec,
            null);

    return segmentNameGenerator.generateSegmentName(DEFAULT_SEQUENCE_ID, minStartTime, maxEndTime);
  }

  private boolean isPinotSegment(File path) {
    try {
      SegmentMetadata segmentMetadata = new SegmentMetadataImpl(path);
      LOGGER.info("Path ({}) is a valid segment ({})", path.getAbsolutePath(), segmentMetadata.getName());
      return true;
    } catch (Exception e) {
      LOGGER.info("Path ({}) is a not valid segment", path.getAbsolutePath());
      return false;
    }
  }

  private void addFilePath(List<String> inputPaths, String path)
      throws Exception {
    File pathFile = new File(path);

    if (!pathFile.exists()) {
      throw new InvalidConfigException("Invalid input path: " + pathFile);
    }

    if (pathFile.isFile()) {
      // If the input is file, add to input path list
      inputPaths.add(pathFile.getAbsolutePath());
      return;
    }

    if (pathFile.isDirectory()) {
      if (isPinotSegment(pathFile)) {
        // If the directory is pinot index dir, add to input path list
        inputPaths.add(pathFile.getAbsolutePath());
      } else {
        // If the directory is not pinot index dir, recursively find the pinot segment file or directory
        File[] files = pathFile.listFiles();
        assert files != null;
        for (File file : files) {
          addFilePath(inputPaths, file.getAbsolutePath());
        }
      }
    }
  }
}
