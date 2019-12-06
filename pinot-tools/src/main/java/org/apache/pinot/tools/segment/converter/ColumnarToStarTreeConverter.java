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
import java.lang.reflect.Field;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.data.StarTreeIndexSpec;
import org.apache.pinot.common.segment.SegmentMetadata;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.segment.creator.SegmentIndexCreationDriver;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.core.segment.index.SegmentMetadataImpl;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;


/**
 * Class to convert Pinot Columnar Segment to Pinot Star Tree Segment
 */
public class ColumnarToStarTreeConverter {
  private static final String TMP_DIR_PREFIX = "_tmp_";

  @Option(name = "-inputDir", required = true, usage = "Path to input directory containing Pinot segments")
  private String _inputDirName = null;

  @Option(name = "-outputDir", required = true, usage = "Path to output directory")
  private String _outputDirName = null;

  @Option(name = "-tableName", required = true, usage = "Table name without type")
  private String _rawTableName = null;

  @Option(name = "-starTreeConfigFile", required = false, usage = "Path to Star Tree configuration file")
  private String _starTreeConfigFileName = null;

  @SuppressWarnings("FieldCanBeLocal")
  @Option(name = "-overwrite", required = false, usage = "Overwrite existing output directory.")
  private boolean _overwrite = false;

  @Option(name = "-help", required = false, help = true, aliases = {"-h"}, usage = "print this message")
  private boolean _help = false;

  /**
   * Convert the specified set of columnar segments into star tree segments.
   * @throws Exception
   */
  public void convert()
      throws Exception {
    File inputDir = new File(_inputDirName);

    if (!inputDir.exists()) {
      System.out.println("Error: Input directory " + _inputDirName + " does not exist.");
      return;
    }

    File outputDir = new File(_outputDirName);
    if (!outputDir.exists()) {
      System.out.println("Error: Output directory " + _outputDirName + " does not exist");
      return;
    }

    File[] files = inputDir.listFiles();
    if (files == null || files.length == 0) {
      System.out.println("Error: Input directory " + _inputDirName + " is empty");
      return;
    }

    for (File file : files) {
      String fileName = file.getName();

      File segment;
      boolean cleanupTempFile = false;

      if (fileName.endsWith("tar.gz") || fileName.endsWith("tgz")) {
        File untarredSegment = new File(outputDir, TMP_DIR_PREFIX + System.currentTimeMillis());
        segment = TarGzCompressionUtils.unTar(file, untarredSegment).get(0);
        cleanupTempFile = true;
      } else {
        segment = file;
      }

      convertSegment(segment);
      if (cleanupTempFile) {
        FileUtils.deleteQuietly(segment.getParentFile());
      }
    }
  }

  /**
   * Helper method to perform the conversion.
   * @param columnarSegment Columnar segment directory to convert
   * @throws Exception
   */
  private void convertSegment(File columnarSegment)
      throws Exception {
    SegmentMetadata segmentMetadata = new SegmentMetadataImpl(columnarSegment);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(segmentMetadata.getSchema());
    config.setDataDir(_inputDirName);
    config.setInputFilePath(columnarSegment.getAbsolutePath());
    config.setFormat(FileFormat.PINOT);
    config.setOutDir(_outputDirName);
    config.setOverwrite(_overwrite);
    config.setTableName(_rawTableName);
    config.setSegmentName(segmentMetadata.getName());

    StarTreeIndexSpec starTreeIndexSpec = null;
    if (_starTreeConfigFileName != null) {
      starTreeIndexSpec = StarTreeIndexSpec.fromFile(new File(_starTreeConfigFileName));
    }
    config.enableStarTreeIndex(starTreeIndexSpec);

    SegmentIndexCreationDriver indexCreator = new SegmentIndexCreationDriverImpl();
    indexCreator.init(config);
    indexCreator.build();
  }

  /**
   * Helper method to print usage at the command line interface.
   */
  private static void printUsage() {
    System.out.println("Usage: ColumnarToStarTreeConverter");
    for (Field field : ColumnarToStarTreeConverter.class.getDeclaredFields()) {

      if (field.isAnnotationPresent(Option.class)) {
        Option option = field.getAnnotation(Option.class);

        System.out
            .println(String.format("\t%-15s: %s (required=%s)", option.name(), option.usage(), option.required()));
      }
    }
  }

  /**
   * Main driver for the converter class.
   *
   * @param args Command line arguments
   * @throws Exception
   */
  public static void main(String[] args)
      throws Exception {
    ColumnarToStarTreeConverter converter = new ColumnarToStarTreeConverter();
    CmdLineParser parser = new CmdLineParser(converter);
    parser.parseArgument(args);

    if (converter._help) {
      printUsage();
      return;
    }

    converter.convert();
  }
}
