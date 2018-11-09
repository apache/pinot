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
package com.linkedin.pinot.tools.segment.converter;

import com.linkedin.pinot.core.segment.name.NormalizedDateSegmentNameGenerator;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.exception.InvalidConfigException;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.minion.rollup.MergeRollupSegmentConverter;
import com.linkedin.pinot.core.minion.rollup.MergeType;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.tools.Command;
import com.linkedin.pinot.tools.admin.command.AbstractBaseAdminCommand;


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

  @Option(name = "-inputPaths", required = true, metaVar = "<String>", usage = "Comma separated input segment files or directories that contains input segments to be merged")
  private String _inputSegmentPaths;

  @Option(name = "-outputPath", required = true, metaVar = "<String>", usage = "Output segment path. This should be different from working directory.")
  private String _outputPath;

  @Option(name = "-tableConfigFilePath", required = true, metaVar = "<String>", usage = "Table config file path.")
  private String _tableConfigFilePath;

  @Option(name = "-schemaFilePath", required = true, metaVar = "<String>", usage = "Schema file path")
  private String _schemaFilePath;

  @Option(name = "-tarOutputSegment", required = true, metaVar = "<String>", usage = "Indicate whether to tar output segment (true, false)")
  private String _tarOutputSegment;

  @Option(name = "-outputSegmentName", required = false, metaVar = "<String>", usage = "The name of output segment file")
  private String _outputSegmentName;

  // TODO: once rollup mode is supported, make this field required
  @Option(name = "-mergeType", required = false, metaVar = "<String>", usage = "Merge type (\"CONCATENATE\" or \"ROLLUP\"). Currently, only \"CONCATENATE\" type is supported.")
  private String _mergeType = DEFAULT_MERGE_TYPE;

  @Option(name = "-workingDirectory", required = false, metaVar = "<String>", usage = "Path for working directory. This directory gets cleaned up after the job")
  private String _workingDirectory;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "SegmentConcatenation";
  }

  @Override
  public boolean execute() throws Exception {
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
          TarGzCompressionUtils.unTar(segmentFile, segmentDir);
          File[] files = segmentDir.listFiles();
          Preconditions.checkState(files != null && files.length == 1);
          File indexDir = files[0];
          inputIndexDirs.add(indexDir);
        }
      }
      LOGGER.info("Input segment paths for segment generator: {}", inputIndexDirs);

      // Read table config
      String tableConfigString =
          new String(Files.readAllBytes(Paths.get(_tableConfigFilePath)), StandardCharsets.UTF_8);
      JSONObject tableConfigJson = new JSONObject(tableConfigString);
      TableConfig tableConfig = TableConfig.fromJSONConfig(tableConfigJson);

      // Read schema
      Schema schema = Schema.fromFile(new File(_schemaFilePath));

      LOGGER.info("Table config: {}", tableConfig);
      LOGGER.info("Schema : {}", schema);

      // Compute segment name if it is not specified
      if (_outputSegmentName == null) {
        _outputSegmentName = getDefaultSegmentName(tableConfig, schema, inputIndexDirs);
      }
      LOGGER.info("Output segment name: {}", _outputSegmentName);

      // Compute segment merge
      // TODO: add support for rollup
      String tableName = TableNameBuilder.extractRawTableName(tableConfig.getTableName());
      MergeRollupSegmentConverter mergeRollupSegmentConverter =
          new MergeRollupSegmentConverter.Builder().setMergeType(_mergeType)
              .setSegmentName(_outputSegmentName)
              .setInputIndexDirs(inputIndexDirs)
              .setWorkingDir(workingDir)
              .setTableName(tableName)
              .build();

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
      boolean tarOutputSegment = Boolean.valueOf(_tarOutputSegment);
      if (tarOutputSegment) {
        File tarredOutputSegmentsDir = new File(workingDir, "tarredOutputSegments");
        Preconditions.checkState(tarredOutputSegmentsDir.mkdir());
        outputSegment = new File(TarGzCompressionUtils.createTarGzOfDirectory(outputSegment.getPath(),
            new File(tarredOutputSegmentsDir, outputSegment.getName()).getPath()));
        FileUtils.moveFile(outputSegment, finalOutputPath);
      } else {
        FileUtils.moveDirectory(outputSegment, finalOutputPath);
      }
      LOGGER.info("Segment has been merged correctly. Output file is located at {}", finalOutputPath);
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

  private String getDefaultSegmentName(TableConfig tableConfig, Schema schema, List<File> inputIndexDirs)
      throws Exception {
    String tableName = TableNameBuilder.extractRawTableName(tableConfig.getTableName());

    // Compute mix/max time from segment metadata
    long minStartTime = Long.MAX_VALUE;
    long maxEndTime = Long.MIN_VALUE;
    for (File indexDir : inputIndexDirs) {
      SegmentMetadata segmentMetadata = new SegmentMetadataImpl(indexDir);
      long currentStartTime = segmentMetadata.getStartTime();
      if (currentStartTime < minStartTime) {
        minStartTime = currentStartTime;
      }

      long currentEndTime = segmentMetadata.getEndTime();
      if (currentEndTime > maxEndTime) {
        maxEndTime = currentEndTime;
      }
    }

    // Fetch time related configurations from schema and table config.
    String pushFrequency = tableConfig.getValidationConfig().getSegmentPushFrequency();
    String timeColumnType = tableConfig.getValidationConfig().getTimeType();
    String pushType = tableConfig.getValidationConfig().getSegmentPushType();
    String timeFormat = schema.getTimeFieldSpec().getOutgoingGranularitySpec().getTimeFormat();

    // Generate the final segment name using segment name generator
    NormalizedDateSegmentNameGenerator segmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(tableName, DEFAULT_SEQUENCE_ID, timeColumnType, pushFrequency, pushType,
            null, null, timeFormat);

    return segmentNameGenerator.generateSegmentName(minStartTime, maxEndTime);
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

  private void addFilePath(List<String> inputPaths, String path) throws Exception {
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
