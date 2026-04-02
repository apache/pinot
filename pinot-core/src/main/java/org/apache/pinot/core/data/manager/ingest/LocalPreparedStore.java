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
package org.apache.pinot.core.data.manager.ingest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.ingest.PreparedStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Local file-based implementation of {@link PreparedStore}.
 *
 * <p>Stores prepared row batches as serialized files under
 * {@code <dataDir>/insert/prepared/<statementId>/<partitionId>/<sequenceNo>}.
 * Each file contains serialized {@link org.apache.pinot.spi.data.readers.GenericRow} data.
 *
 * <p>All writes are fsynced to disk before returning to ensure durability. This class is
 * thread-safe for concurrent access across different statements. Concurrent access to the same
 * statement is not expected.
 */
public class LocalPreparedStore implements PreparedStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalPreparedStore.class);
  private static final String PREPARED_DIR = "prepared";

  private File _preparedBaseDir;

  @Override
  public void init(PinotConfiguration config, File dataDir) {
    _preparedBaseDir = new File(new File(dataDir, "insert"), PREPARED_DIR);
    if (!_preparedBaseDir.exists() && !_preparedBaseDir.mkdirs()) {
      throw new RuntimeException("Failed to create prepared store directory: " + _preparedBaseDir);
    }
    LOGGER.info("Initialized LocalPreparedStore with directory: {}", _preparedBaseDir);
  }

  /**
   * Initializes the store with an explicit base directory. Useful for testing.
   *
   * @param preparedBaseDir the base directory for prepared data files
   */
  public void init(File preparedBaseDir) {
    _preparedBaseDir = preparedBaseDir;
    if (!_preparedBaseDir.exists() && !_preparedBaseDir.mkdirs()) {
      throw new RuntimeException("Failed to create prepared store directory: " + _preparedBaseDir);
    }
  }

  @Override
  public void store(String statementId, int partitionId, long sequenceNo, byte[] data) {
    File targetFile = getFilePath(statementId, partitionId, sequenceNo);
    File parentDir = targetFile.getParentFile();
    if (!parentDir.exists() && !parentDir.mkdirs()) {
      throw new RuntimeException("Failed to create directory: " + parentDir);
    }
    try (FileOutputStream fos = new FileOutputStream(targetFile);
         FileChannel channel = fos.getChannel()) {
      fos.write(data);
      channel.force(true);
    } catch (IOException e) {
      throw new RuntimeException("Failed to store prepared data for statement " + statementId, e);
    }
  }

  @Override
  public byte[] load(String statementId, int partitionId, long sequenceNo) {
    File targetFile = getFilePath(statementId, partitionId, sequenceNo);
    if (!targetFile.exists()) {
      return null;
    }
    try {
      return Files.readAllBytes(targetFile.toPath());
    } catch (IOException e) {
      throw new RuntimeException("Failed to load prepared data for statement " + statementId, e);
    }
  }

  @Override
  public List<String> listPreparedStatements() {
    List<String> statements = new ArrayList<>();
    if (!_preparedBaseDir.exists()) {
      return statements;
    }
    File[] stmtDirs = _preparedBaseDir.listFiles(File::isDirectory);
    if (stmtDirs != null) {
      for (File stmtDir : stmtDirs) {
        statements.add(stmtDir.getName());
      }
    }
    return statements;
  }

  @Override
  public void cleanup(String statementId) {
    File stmtDir = new File(_preparedBaseDir, statementId);
    if (stmtDir.exists()) {
      try {
        FileUtils.deleteDirectory(stmtDir);
        LOGGER.info("Cleaned up prepared data for statement: {}", statementId);
      } catch (IOException e) {
        LOGGER.error("Failed to cleanup prepared data for statement: {}", statementId, e);
      }
    }
  }

  /**
   * Returns the base directory for prepared data. Useful for testing and recovery.
   */
  public File getPreparedBaseDir() {
    return _preparedBaseDir;
  }

  private File getFilePath(String statementId, int partitionId, long sequenceNo) {
    return new File(_preparedBaseDir,
        statementId + File.separator + partitionId + File.separator + sequenceNo);
  }
}
