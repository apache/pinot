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
package org.apache.pinot.controller.recommender.data.writer;

import java.io.File;
import java.util.Objects;
import org.apache.pinot.controller.recommender.data.generator.DataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class FileWriter implements Writer {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileWriter.class);

  protected FileWriterSpec _spec;
  @Override
  public void init(WriterSpec spec) {
    _spec = (FileWriterSpec) spec;
  }

  @Override
  public void write()
      throws Exception {
    long totalDocs = _spec.getTotalDocs();
    final long docsPerFile = (long) Math.ceil((double) totalDocs / _spec.getNumFiles());
    final String extension = getExtension() == null ? "" : String.format(".%s", getExtension());
    long ingestedDocs = 0;
    int fileIndex = 0;
    while (ingestedDocs < totalDocs) {
      try (java.io.FileWriter writer =
          new java.io.FileWriter(new File(_spec.getBaseDir(), String.format("output_%d%s", fileIndex, extension)))) {
        preprocess(writer);
        for (int j = 0; j < docsPerFile && ingestedDocs < totalDocs; j++) {
          String appendString = generateRow(_spec.getGenerator());
          writer.append(appendString).append('\n');
          ingestedDocs++;
        }
      }
      fileIndex++;
    }
  }

  protected void preprocess(java.io.FileWriter writer)
      throws Exception {
  }

  protected String getExtension() {
    return null;
  }

  @Override
  public void cleanup() {
    File baseDir = new File(_spec.getBaseDir().toURI());
    for (File file : Objects.requireNonNull(baseDir.listFiles())) {
      if (!file.delete()) {
        LOGGER.error("Unable to delete file {}", file.getAbsolutePath());
      }
    }
    if (!baseDir.delete()) {
      LOGGER.error("Unable to delete directory {}", baseDir.getAbsolutePath());
    }
  }

  protected abstract String generateRow(DataGenerator generator);
}
