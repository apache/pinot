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
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.core.segment.processing.framework.SegmentProcessorConfig;
import org.apache.pinot.core.segment.processing.framework.SegmentProcessorFramework;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.segment.processor.SegmentProcessorFrameworkSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Command to run {@link org.apache.pinot.core.segment.processing.framework.SegmentProcessorFramework}
 */
@CommandLine.Command(name = "SegmentProcessorFramework", description = "Runs the SegmentProcessorFramework",
    mixinStandardHelpOptions = true)
public class SegmentProcessorFrameworkCommand extends AbstractBaseAdminCommand implements Command {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentProcessorFrameworkCommand.class);

  @CommandLine.Option(names = {"-segmentProcessorFrameworkSpec"}, required = true,
      description = "Path to SegmentProcessorFrameworkSpec json file")
  private String _segmentProcessorFrameworkSpec;

  @Override
  public String getName() {
    return "SegmentProcessorFramework";
  }

  @Override
  public String toString() {
    return ("SegmentProcessorFramework -segmentProcessorFrameworkSpec " + _segmentProcessorFrameworkSpec);
  }

  @Override
  public void cleanup() {
  }

  @Override
  public boolean execute()
      throws Exception {
    PluginManager.get().init();

    SegmentProcessorFrameworkSpec segmentProcessorFrameworkSpec =
        JsonUtils.fileToObject(new File(_segmentProcessorFrameworkSpec), SegmentProcessorFrameworkSpec.class);

    File inputSegmentsDir = new File(segmentProcessorFrameworkSpec.getInputSegmentsDir());
    File outputSegmentsDir = new File(segmentProcessorFrameworkSpec.getOutputSegmentsDir());
    File workingDir = new File(outputSegmentsDir, "tmp-" + UUID.randomUUID());
    File untarredSegmentsDir = new File(workingDir, "untarred_segments");
    FileUtils.forceMkdir(untarredSegmentsDir);
    File[] segmentDirs = inputSegmentsDir.listFiles();
    Preconditions
        .checkState(segmentDirs != null && segmentDirs.length > 0, "Failed to find files under input segments dir: %s",
            inputSegmentsDir.getAbsolutePath());
    List<RecordReader> recordReaders = new ArrayList<>(segmentDirs.length);
    for (File segmentDir : segmentDirs) {
      String fileName = segmentDir.getName();

      File finalSegmentDir;
      // Untar the segments if needed
      if (!segmentDir.isDirectory()) {
        if (fileName.endsWith(".tar.gz") || fileName.endsWith(".tgz")) {
          finalSegmentDir = TarGzCompressionUtils.untar(segmentDir, untarredSegmentsDir).get(0);
        } else {
          throw new IllegalStateException("Unsupported segment format: " + segmentDir.getAbsolutePath());
        }
      } else {
        finalSegmentDir = segmentDir;
      }

      PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader();
      // NOTE: Do not fill null field with default value to be consistent with other record readers
      recordReader.init(finalSegmentDir, null, null, true);
      recordReaders.add(recordReader);
    }

    TableConfig tableConfig =
        JsonUtils.fileToObject(new File(segmentProcessorFrameworkSpec.getTableConfigFile()), TableConfig.class);
    Schema schema = Schema.fromFile(new File(segmentProcessorFrameworkSpec.getSchemaFile()));
    SegmentProcessorConfig segmentProcessorConfig =
        new SegmentProcessorConfig.Builder().setTableConfig(tableConfig).setSchema(schema)
            .setTimeHandlerConfig(segmentProcessorFrameworkSpec.getTimeHandlerConfig())
            .setPartitionerConfigs(segmentProcessorFrameworkSpec.getPartitionerConfigs())
            .setMergeType(segmentProcessorFrameworkSpec.getMergeType())
            .setAggregationTypes(segmentProcessorFrameworkSpec.getAggregationTypes())
            .setSegmentConfig(segmentProcessorFrameworkSpec.getSegmentConfig()).build();

    SegmentProcessorFramework framework =
        new SegmentProcessorFramework(recordReaders, segmentProcessorConfig, workingDir);
    try {
      LOGGER.info("Starting processing segments via SegmentProcessingFramework");
      List<File> outputSegmentDirs = framework.process();
      for (File outputSegmentDir : outputSegmentDirs) {
        FileUtils.moveDirectory(outputSegmentDir, new File(outputSegmentsDir, outputSegmentDir.getName()));
      }
      LOGGER.info("Finished processing segments via SegmentProcessingFramework");
    } catch (Exception e) {
      LOGGER.error("Caught exception when running SegmentProcessingFramework. Exiting", e);
      return false;
    } finally {
      for (RecordReader recordReader : recordReaders) {
        recordReader.close();
      }
      FileUtils.deleteQuietly(workingDir);
    }
    return true;
  }
}
