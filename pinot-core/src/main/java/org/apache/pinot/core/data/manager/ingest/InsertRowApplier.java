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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.ingest.PreparedStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Applies committed insert data to Pinot's mutable segment path.
 *
 * <p>On commit, reads prepared batches from the {@link PreparedStore} and provides them for
 * indexing into mutable segments. Tracks applied statement IDs in a persistent file to enable
 * idempotent replay after server restart.
 *
 * <p>The applied-statement tracking file is stored at
 * {@code <dataDir>/insert/applied_statements.log}.
 *
 * <p>This class is thread-safe for concurrent apply operations on different statements. Concurrent
 * apply on the same statement is not expected.
 */
public class InsertRowApplier {

  private static final Logger LOGGER = LoggerFactory.getLogger(InsertRowApplier.class);
  private static final String APPLIED_STATEMENTS_FILE = "applied_statements.log";

  private final PreparedStore _preparedStore;
  private final File _appliedStatementsFile;
  private final Set<String> _appliedStatements;
  private final Object _fileLock = new Object();

  /**
   * Creates a new insert row applier.
   *
   * @param preparedStore the prepared store to read batches from
   * @param dataDir the data directory for storing the applied-statements tracking file
   */
  public InsertRowApplier(PreparedStore preparedStore, File dataDir) {
    _preparedStore = preparedStore;
    File insertDir = new File(dataDir, "insert");
    if (!insertDir.exists() && !insertDir.mkdirs()) {
      throw new RuntimeException("Failed to create insert directory: " + insertDir);
    }
    _appliedStatementsFile = new File(insertDir, APPLIED_STATEMENTS_FILE);
    _appliedStatements = loadAppliedStatements();
  }

  /**
   * Applies the committed data for the given statement and partition. Returns the deserialized
   * rows that should be indexed into the mutable segment.
   *
   * <p>This method is idempotent: if the statement has already been applied (tracked in the
   * applied-statements file), it returns {@code null} to indicate no action is needed.
   *
   * @param statementId the statement identifier
   * @param partitionId the partition identifier
   * @param sequenceNo the sequence number
   * @return the list of rows to index, or {@code null} if already applied
   * @throws IOException if reading or deserializing the prepared data fails
   */
  public List<GenericRow> apply(String statementId, int partitionId, long sequenceNo)
      throws IOException {
    String applyKey = statementId + ":" + partitionId + ":" + sequenceNo;

    if (_appliedStatements.contains(applyKey)) {
      LOGGER.info("Statement {} partition {} seq {} already applied, skipping", statementId, partitionId, sequenceNo);
      return null;
    }

    byte[] data = _preparedStore.load(statementId, partitionId, sequenceNo);
    if (data == null) {
      LOGGER.warn("No prepared data found for statement {} partition {} seq {}", statementId, partitionId, sequenceNo);
      return null;
    }

    List<GenericRow> rows = GenericRowSerializer.deserializeRows(data);
    LOGGER.info("Deserialized {} rows for statement {} partition {} seq {}", rows.size(), statementId, partitionId,
        sequenceNo);

    // Note: do NOT mark as applied here. The caller must invoke confirmApplied() after
    // successfully indexing the rows. Marking before indexing would cause data loss if
    // indexing fails — the batch would be permanently skipped on subsequent recovery.

    return rows;
  }

  /**
   * Confirms that a batch has been successfully indexed and should not be replayed again.
   * Must be called by the caller after the rows returned by {@link #apply} have been
   * successfully indexed into the mutable segment.
   *
   * @param statementId the statement identifier
   * @param partitionId the partition identifier
   * @param sequenceNo the sequence number
   */
  public void confirmApplied(String statementId, int partitionId, long sequenceNo) {
    String applyKey = statementId + ":" + partitionId + ":" + sequenceNo;
    markApplied(applyKey);
    LOGGER.info("Confirmed apply for statement {} partition {} seq {}", statementId, partitionId, sequenceNo);
  }

  /**
   * Returns whether a given statement/partition/sequence has already been applied.
   *
   * @param statementId the statement identifier
   * @param partitionId the partition identifier
   * @param sequenceNo the sequence number
   * @return {@code true} if already applied
   */
  public boolean isApplied(String statementId, int partitionId, long sequenceNo) {
    String applyKey = statementId + ":" + partitionId + ":" + sequenceNo;
    return _appliedStatements.contains(applyKey);
  }

  private void markApplied(String applyKey) {
    synchronized (_fileLock) {
      _appliedStatements.add(applyKey);
      try (BufferedWriter writer = new BufferedWriter(new FileWriter(_appliedStatementsFile, true))) {
        writer.write(applyKey);
        writer.newLine();
        writer.flush();
      } catch (IOException e) {
        LOGGER.error("Failed to record applied statement: {}", applyKey, e);
      }
    }
  }

  /**
   * Removes tracking entries for the given statement from the in-memory set and rewrites the
   * applied-statements file. Called during GC to prevent the tracking file from growing without
   * bound.
   *
   * @param statementId the statement identifier to remove tracking entries for
   */
  public void cleanAppliedEntries(String statementId) {
    String prefix = statementId + ":";
    synchronized (_fileLock) {
      boolean removed = _appliedStatements.removeIf(key -> key.startsWith(prefix));
      if (removed) {
        rewriteAppliedStatementsFile();
        LOGGER.info("Cleaned applied tracking entries for statement: {}", statementId);
      }
    }
  }

  /**
   * Rewrites the applied-statements file from the current in-memory set.
   * Must be called under {@code _fileLock}.
   */
  private void rewriteAppliedStatementsFile() {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(_appliedStatementsFile, false))) {
      for (String key : _appliedStatements) {
        writer.write(key);
        writer.newLine();
      }
      writer.flush();
    } catch (IOException e) {
      LOGGER.error("Failed to rewrite applied statements file: {}", _appliedStatementsFile, e);
    }
  }

  private Set<String> loadAppliedStatements() {
    Set<String> applied = new HashSet<>();
    if (!_appliedStatementsFile.exists()) {
      return applied;
    }
    try (BufferedReader reader = new BufferedReader(new FileReader(_appliedStatementsFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (!line.isEmpty()) {
          applied.add(line);
        }
      }
    } catch (IOException e) {
      LOGGER.error("Failed to load applied statements from: {}", _appliedStatementsFile, e);
    }
    return applied;
  }
}
