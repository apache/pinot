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

package org.apache.pinot.controller.recommender.data;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.recommender.data.generator.DataGenerator;
import org.apache.pinot.controller.recommender.data.writer.AvroWriter;
import org.apache.pinot.controller.recommender.data.writer.AvroWriterSpec;
import org.apache.pinot.controller.recommender.data.writer.CsvWriter;
import org.apache.pinot.controller.recommender.data.writer.FileWriterSpec;
import org.apache.pinot.controller.recommender.data.writer.JsonWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DataGenerationHelpers {

  private DataGenerationHelpers() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(DataGenerationHelpers.class);

  public static void generateAvro(DataGenerator generator, long totalDocs, int numFiles, String outDir,
      boolean isOverrideOutDir) throws Exception {
    AvroWriter avroWriter = new AvroWriter();
    avroWriter.init(new AvroWriterSpec(generator, handleOutDir(outDir, isOverrideOutDir), totalDocs, numFiles));
    avroWriter.write();
  }

  public static void generateCsv(DataGenerator generator, long totalDocs, int numFiles, String outDir,
      boolean isOverrideOutDir) throws Exception {
    CsvWriter csvWriter = new CsvWriter();
    csvWriter.init(new FileWriterSpec(generator, handleOutDir(outDir, isOverrideOutDir), totalDocs, numFiles));
    csvWriter.write();
  }

  public static void generateJson(DataGenerator generator, long totalDocs, int numFiles, String outDir,
      boolean isOverrideOutDir) throws Exception {
    JsonWriter jsonWriter = new JsonWriter();
    jsonWriter.init(new FileWriterSpec(generator, handleOutDir(outDir, isOverrideOutDir), totalDocs, numFiles));
    jsonWriter.write();
  }

  private static File handleOutDir(String outDir, boolean isOverrideOutDir)
      throws IOException {
    File dir = new File(outDir);
    if (dir.exists() && !isOverrideOutDir) {
      LOGGER.error("output directory already exists, and override is set to false");
      throw new RuntimeException("output directory exists");
    }
    if (dir.exists()) {
      FileUtils.deleteDirectory(dir);
    }
    dir.mkdir();
    return dir;
  }
}
