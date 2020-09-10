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

import java.io.File;
import org.apache.pinot.core.segment.processing.framework.SegmentProcessorConfig;
import org.apache.pinot.core.segment.processing.framework.SegmentProcessorFramework;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.segment.processor.SegmentProcessorFrameworkSpec;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Command to run {@link org.apache.pinot.core.segment.processing.framework.SegmentProcessorFramework}
 */
public class SegmentProcessorFrameworkCommand extends AbstractBaseAdminCommand implements Command {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentProcessorFrameworkCommand.class);

  @Option(name = "-segmentProcessorFrameworkSpec", required = true, metaVar = "<String>", usage = "Path to SegmentProcessorFrameworkSpec json file")
  private String _segmentProcessorFrameworkSpec;

  @Option(name = "-help", help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean getHelp() {
    return _help;
  }

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
  public String description() {
    return "Runs the SegmentProcessorFramework";
  }

  @Override
  public boolean execute()
      throws Exception {

    SegmentProcessorFrameworkSpec segmentProcessorFrameworkSpec =
        JsonUtils.fileToObject(new File(_segmentProcessorFrameworkSpec), SegmentProcessorFrameworkSpec.class);

    File inputSegmentsDir = new File(segmentProcessorFrameworkSpec.getInputSegmentsDir());
    File outputSegmentsDir = new File(segmentProcessorFrameworkSpec.getOutputSegmentsDir());
    if (!outputSegmentsDir.exists()) {
      if (!outputSegmentsDir.mkdirs()) {
        throw new RuntimeException(
            "Did not find output directory, and could not create it either: " + segmentProcessorFrameworkSpec
                .getOutputSegmentsDir());
      }
    }

    PluginManager.get().init();

    Schema schema = Schema.fromFile(new File(segmentProcessorFrameworkSpec.getSchemaFile()));
    TableConfig tableConfig =
        JsonUtils.fileToObject(new File(segmentProcessorFrameworkSpec.getTableConfigFile()), TableConfig.class);
    SegmentProcessorConfig segmentProcessorConfig =
        new SegmentProcessorConfig.Builder().setSchema(schema).setTableConfig(tableConfig)
            .setRecordTransformerConfig(segmentProcessorFrameworkSpec.getRecordTransformerConfig())
            .setRecordFilterConfig(segmentProcessorFrameworkSpec.getRecordFilterConfig())
            .setPartitionerConfig(segmentProcessorFrameworkSpec.getPartitionerConfig())
            .setCollectorConfig(segmentProcessorFrameworkSpec.getCollectorConfig())
            .setSegmentConfig(segmentProcessorFrameworkSpec.getSegmentConfig()).build();

    SegmentProcessorFramework framework =
        new SegmentProcessorFramework(inputSegmentsDir, segmentProcessorConfig, outputSegmentsDir);
    try {
      LOGGER.info("Starting processing segments via SegmentProcessingFramework");
      framework.processSegments();
      LOGGER.info("Finished processing segments via SegmentProcessingFramework");
    } catch (Exception e) {
      LOGGER.error("Caught exception when running SegmentProcessingFramework. Exiting", e);
      return false;
    } finally {
      framework.cleanup();
    }
    return true;
  }
}
