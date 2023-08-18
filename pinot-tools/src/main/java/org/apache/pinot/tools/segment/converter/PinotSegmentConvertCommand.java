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

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.tools.AbstractBaseCommand;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * The <code>PinotSegmentConvertCommand</code> class provides tools to convert Pinot segments into another format.
 * <p>Currently support converting Pinot segments to:
 * <ul>
 *   <li>AVRO format</li>
 *   <li>CSV format</li>
 *   <li>JSON format</li>
 * </ul>
 */
@SuppressWarnings("FieldCanBeLocal")
@CommandLine.Command(name = "PinotSegmentConvert", description = "Convert Pinot segments to another format such as "
                                                                 + "AVRO/CSV/JSON.", usageHelpAutoWidth = true)
public class PinotSegmentConvertCommand extends AbstractBaseCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentConvertCommand.class);
  private static final String TEMP_DIR_NAME = "temp";

  @CommandLine.Option(names = {"-dataDir"}, required = true,
      description = "Path to data directory containing Pinot segments.")
  private String _dataDir;

  @CommandLine.Option(names = {"-outputDir"}, required = true, description = "Path to output directory.")
  private String _outputDir;

  @CommandLine.Option(names = {"-outputFormat"}, required = true,
      description = "Format to convert to (AVRO/CSV/JSON).")
  private String _outputFormat;

  @CommandLine.Option(names = {"-csvDelimiter"}, required = false, description = "CSV delimiter (default ',').")
  private char _csvDelimiter = ',';

  @CommandLine.Option(names = {"-csvListDelimiter"}, required = false,
      description = "CSV List delimiter for multi-value columns (default ';').")
  private char _csvListDelimiter = ';';

  @CommandLine.Option(names = {"-csvWithHeader"}, required = false, description = "Print CSV Header (default false).")
  private boolean _csvWithHeader;

  @CommandLine.Option(names = {"-overwrite"}, required = false,
      description = "Overwrite the existing file (default false).")
  private boolean _overwrite;

  @Override
  public boolean execute()
      throws Exception {
    // Make sure output directory is empty.
    File outputDir = new File(_outputDir);
    if (outputDir.exists()) {
      if (_overwrite) {
        if (!FileUtils.deleteQuietly(outputDir)) {
          throw new RuntimeException(
              "Output directory: " + outputDir.getAbsolutePath() + " already exists and cannot be deleted.");
        }
      } else {
        throw new RuntimeException(
            "Output directory: " + outputDir.getAbsolutePath() + " already exists and overwrite flag is not set.");
      }
    }
    if (!outputDir.mkdirs()) {
      throw new RuntimeException("Output directory: " + outputDir.getAbsolutePath() + " cannot be created.");
    }

    File tempDir = new File(outputDir, TEMP_DIR_NAME);
    try {
      // Add all segments to the segment path map.
      Map<String, String> segmentPath = new HashMap<>();
      File dataDir = new File(_dataDir);
      File[] files = dataDir.listFiles();
      if (files == null || files.length == 0) {
        throw new RuntimeException("Data directory does not contain any files.");
      }
      for (File file : files) {
        String fileName = file.getName();
        if (file.isDirectory()) {
          // Uncompressed segment.
          if (segmentPath.containsKey(fileName)) {
            throw new RuntimeException("Multiple segments with the same segment name: " + fileName);
          }
          segmentPath.put(fileName, file.getAbsolutePath());
        } else if (fileName.toLowerCase().endsWith(".tar.gz") || fileName.toLowerCase().endsWith(".tgz")) {
          // Compressed segment.
          File segment = TarGzCompressionUtils.untar(file, new File(tempDir, fileName)).get(0);
          String segmentName = segment.getName();
          if (segmentPath.containsKey(segmentName)) {
            throw new RuntimeException("Multiple segments with the same segment name: " + fileName);
          }
          segmentPath.put(segmentName, segment.getAbsolutePath());
        }
      }

      // Do the conversion according to the output format.
      for (Map.Entry<String, String> entry : segmentPath.entrySet()) {
        String segmentName = entry.getKey();
        String inputPath = entry.getValue();
        String outputPath = new File(outputDir, segmentName).getAbsolutePath();
        switch (FileFormat.valueOf(_outputFormat.toUpperCase())) {
          case AVRO:
            outputPath += ".avro";
            new PinotSegmentToAvroConverter(inputPath, outputPath).convert();
            break;
          case CSV:
            outputPath += ".csv";
            new PinotSegmentToCsvConverter(inputPath, outputPath, _csvDelimiter, _csvDelimiter,
                _csvWithHeader).convert();
            break;
          case JSON:
            outputPath += ".json";
            new PinotSegmentToJsonConverter(inputPath, outputPath).convert();
            break;
          default:
            throw new RuntimeException("Unsupported conversion to file format: " + _outputFormat);
        }
        LOGGER.info("Finish converting segment: {} into file: {}", segmentName, outputPath);
      }

      return true;
    } finally {
      FileUtils.deleteQuietly(tempDir);
    }
  }
}
