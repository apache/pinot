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
package org.apache.pinot.hadoop.job.preprocess;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.pinot.hadoop.utils.preprocess.DataFileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DataPreprocessingHelperFactory {
  private DataPreprocessingHelperFactory() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(DataPreprocessingHelperFactory.class);

  public static DataPreprocessingHelper generateDataPreprocessingHelper(Path inputPaths, Path outputPath)
      throws IOException {
    final List<Path> avroFiles = DataFileUtils.getDataFiles(inputPaths, DataFileUtils.AVRO_FILE_EXTENSION);
    final List<Path> orcFiles = DataFileUtils.getDataFiles(inputPaths, DataFileUtils.ORC_FILE_EXTENSION);

    int numAvroFiles = avroFiles.size();
    int numOrcFiles = orcFiles.size();
    Preconditions
        .checkState(numAvroFiles == 0 || numOrcFiles == 0,
            "Cannot preprocess mixed AVRO files: %s and ORC files: %s in directories: %s", avroFiles, orcFiles,
            inputPaths);
    Preconditions
        .checkState(numAvroFiles > 0 || numOrcFiles > 0, "Failed to find any AVRO or ORC file in directories: %s",
            inputPaths);

    if (numAvroFiles > 0) {
      LOGGER.info("Found AVRO files: {} in directories: {}", avroFiles, inputPaths);
      return new AvroDataPreprocessingHelper(avroFiles, outputPath);
    } else {
      LOGGER.info("Found ORC files: {} in directories: {}", orcFiles, inputPaths);
      return new OrcDataPreprocessingHelper(orcFiles, outputPath);
    }
  }
}
