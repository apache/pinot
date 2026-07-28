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
package org.apache.pinot.compat;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Verifies that at least one file matching a glob in a system-property directory contains an expected text fragment.
 * Instances are configured and invoked serially by the compatibility runner and are not thread-safe.
 */
public class FileContainsOp extends BaseOp {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileContainsOp.class);
  private static final int MAX_ATTEMPTS = 10;
  private static final long RETRY_DELAY_MS = 100L;

  private String _directorySystemProperty;
  private String _fileNameGlob;
  private String _expectedTextContains;

  public FileContainsOp() {
    super(OpType.FILE_CONTAINS_OP);
  }

  public String getDirectorySystemProperty() {
    return _directorySystemProperty;
  }

  public void setDirectorySystemProperty(String directorySystemProperty) {
    _directorySystemProperty = directorySystemProperty;
  }

  public String getFileNameGlob() {
    return _fileNameGlob;
  }

  public void setFileNameGlob(String fileNameGlob) {
    _fileNameGlob = fileNameGlob;
  }

  public String getExpectedTextContains() {
    return _expectedTextContains;
  }

  public void setExpectedTextContains(String expectedTextContains) {
    _expectedTextContains = expectedTextContains;
  }

  @Override
  boolean runOp(int generationNumber) {
    if (isBlank(_directorySystemProperty) || isBlank(_fileNameGlob) || isBlank(_expectedTextContains)) {
      LOGGER.error(
          "directorySystemProperty, fileNameGlob, and expectedTextContains must all be configured and non-blank");
      return false;
    }
    String directoryName = System.getProperty(_directorySystemProperty);
    if (isBlank(directoryName)) {
      LOGGER.error("Required log-directory system property {} is not configured", _directorySystemProperty);
      return false;
    }
    Path directory = Path.of(directoryName);
    if (!Files.isDirectory(directory)) {
      LOGGER.error("Configured log directory does not exist: {}", directory);
      return false;
    }

    try {
      for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
        if (containsExpectedText(directory, _fileNameGlob, _expectedTextContains)) {
          return true;
        }
        if (attempt < MAX_ATTEMPTS) {
          Thread.sleep(RETRY_DELAY_MS);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.error("Interrupted while waiting for expected text in {} files under {}", _fileNameGlob, directory);
      return false;
    } catch (IOException e) {
      LOGGER.error("Failed to inspect {} files under {}", _fileNameGlob, directory, e);
      return false;
    }
    LOGGER.error("No {} file under {} contained expected text: {}", _fileNameGlob, directory, _expectedTextContains);
    return false;
  }

  static boolean containsExpectedText(Path directory, String fileNameGlob, String expectedText)
      throws IOException {
    try (DirectoryStream<Path> paths = Files.newDirectoryStream(directory, fileNameGlob)) {
      for (Path path : paths) {
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
          String line;
          while ((line = reader.readLine()) != null) {
            if (line.contains(expectedText)) {
              return true;
            }
          }
        }
      }
    }
    return false;
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }
}
