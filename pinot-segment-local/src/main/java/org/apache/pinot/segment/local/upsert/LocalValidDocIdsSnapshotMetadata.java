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
package org.apache.pinot.segment.local.upsert;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Metadata class to store table configuration information alongside local validDocIds snapshots.
 * This is used to check if the local validDocIds snapshots are compatible with the current table
 * configuration before using them during server restart/preload.
 *
 * The metadata file is stored at the table partition level, so that all segments in the same partition
 * share the same metadata. The file is named "upsert.snapshot.metadata" and is stored in the table
 * index directory.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LocalValidDocIdsSnapshotMetadata {
  private static final Logger LOGGER = LoggerFactory.getLogger(LocalValidDocIdsSnapshotMetadata.class);

  public static final String METADATA_FILE_NAME = "upsert.snapshot.metadata.partition.";
  public static final int CURRENT_VERSION = 1;

  // Version for backward compatibility
  private int _version = CURRENT_VERSION;

  // Partition information
  private int _partitionId;

  // Timestamp when the metadata was created
  private long _creationTime;

  // Primary key columns - changes to these invalidate the snapshot
  private List<String> _primaryKeyColumns;

  // Comparison columns - changes to these invalidate the snapshot
  private List<String> _comparisonColumns;

  // Delete record column - changes to this invalidate the snapshot
  private String _deleteRecordColumn;

  // Hash function used to hash primary keys
  private HashFunction _hashFunction;

  // Metadata TTL - changes to this invalidate the snapshot
  private double _metadataTTL;

  // Deleted keys TTL - changes to this invalidate the snapshot
  private double _deletedKeysTTL;

  // Upsert mode (FULL, PARTIAL)
  private UpsertConfig.Mode _upsertMode;

  // Partial upsert strategies (only applicable for PARTIAL mode)
  private Map<String, UpsertConfig.Strategy> _partialUpsertStrategies;

  // Default partial upsert strategy (only applicable for PARTIAL mode)
  private UpsertConfig.Strategy _defaultPartialUpsertStrategy;

  public LocalValidDocIdsSnapshotMetadata() {
  }

  /**
   * Creates metadata from the given UpsertContext and TableConfig.
   */
  public static LocalValidDocIdsSnapshotMetadata fromUpsertContext(int partitionId, UpsertContext context) {
    LocalValidDocIdsSnapshotMetadata metadata = new LocalValidDocIdsSnapshotMetadata();
    metadata.setVersion(CURRENT_VERSION);
    metadata.setPartitionId(partitionId);
    metadata.setCreationTime(System.currentTimeMillis());
    metadata.setPrimaryKeyColumns(context.getPrimaryKeyColumns());
    metadata.setComparisonColumns(context.getComparisonColumns());
    metadata.setDeleteRecordColumn(context.getDeleteRecordColumn());
    metadata.setHashFunction(context.getHashFunction());
    metadata.setMetadataTTL(context.getMetadataTTL());
    metadata.setDeletedKeysTTL(context.getDeletedKeysTTL());

    TableConfig tableConfig = context.getTableConfig();
    if (tableConfig != null) {
      UpsertConfig.Mode upsertMode = tableConfig.getUpsertMode();
      metadata.setUpsertMode(upsertMode);
      if (upsertMode == UpsertConfig.Mode.PARTIAL) {
        UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
        if (upsertConfig != null) {
          metadata.setPartialUpsertStrategies(upsertConfig.getPartialUpsertStrategies());
          metadata.setDefaultPartialUpsertStrategy(upsertConfig.getDefaultPartialUpsertStrategy());
        }
      }
    }

    return metadata;
  }

  /**
   * Reads the metadata from the given directory for the specified partition.
   *
   * @param tableIndexDir the table index directory
   * @param partitionId the partition ID
   * @return the metadata, or null if the file does not exist or cannot be read
   */
  @Nullable
  public static LocalValidDocIdsSnapshotMetadata fromDirectory(File tableIndexDir, int partitionId) {
    File metadataFile = new File(tableIndexDir, METADATA_FILE_NAME + partitionId);
    if (!metadataFile.exists()) {
      LOGGER.debug("Metadata file {} does not exist", metadataFile.getAbsolutePath());
      return null;
    }
    try {
      return JsonUtils.fileToObject(metadataFile, LocalValidDocIdsSnapshotMetadata.class);
    } catch (Exception e) {
      LOGGER.warn("Failed to read metadata file {}: {}", metadataFile.getAbsolutePath(), e.getMessage());
      return null;
    }
  }

  /**
   * Persists the metadata to the given directory.
   *
   * @param tableIndexDir the table index directory
   * @throws IOException if the file cannot be written
   */
  public void persist(File tableIndexDir)
      throws IOException {
    File metadataFile = new File(tableIndexDir, METADATA_FILE_NAME + _partitionId);
    FileUtils.write(metadataFile, JsonUtils.objectToString(this), StandardCharsets.UTF_8);
    LOGGER.debug("Persisted upsert snapshot metadata to {}", metadataFile.getAbsolutePath());
  }

  /**
   * Checks if this metadata is compatible with the given UpsertContext.
   * Returns true if the snapshots can be safely used for preloading.
   *
   * @param context the current UpsertContext
   * @param tableName the table name (for logging)
   * @return true if compatible, false otherwise
   */
  @JsonIgnore
  public boolean isCompatibleWith(UpsertContext context, String tableName) {
    // Check primary key columns
    if (!Objects.equals(_primaryKeyColumns, context.getPrimaryKeyColumns())) {
      LOGGER.info("Previous snapshot used primary keys: {} different from current: {} for table: {}, partition: {}",
          _primaryKeyColumns, context.getPrimaryKeyColumns(), tableName, _partitionId);
      return false;
    }

    // Check comparison columns
    if (!Objects.equals(_comparisonColumns, context.getComparisonColumns())) {
      LOGGER.info("Previous snapshot used comparison columns: {} different from current: {} for table: {}, "
          + "partition: {}", _comparisonColumns, context.getComparisonColumns(), tableName, _partitionId);
      return false;
    }

    // Check delete record column
    if (!StringUtils.equals(_deleteRecordColumn, context.getDeleteRecordColumn())) {
      LOGGER.info("Previous snapshot used deleteRecordColumn: {} different from current: {} for table: {}, "
          + "partition: {}", _deleteRecordColumn, context.getDeleteRecordColumn(), tableName, _partitionId);
      return false;
    }

    // Check hash function
    if (_hashFunction != context.getHashFunction()) {
      LOGGER.info("Previous snapshot used hash function: {} different from current: {} for table: {}, partition: {}",
          _hashFunction, context.getHashFunction(), tableName, _partitionId);
      return false;
    }

    // Check metadata TTL
    if (Double.compare(_metadataTTL, context.getMetadataTTL()) != 0) {
      LOGGER.info("Previous snapshot used metadataTTL: {} different from current: {} for table: {}, partition: {}",
          _metadataTTL, context.getMetadataTTL(), tableName, _partitionId);
      return false;
    }

    // Check deleted keys TTL
    if (Double.compare(_deletedKeysTTL, context.getDeletedKeysTTL()) != 0) {
      LOGGER.info("Previous snapshot used deletedKeysTTL: {} different from current: {} for table: {}, partition: {}",
          _deletedKeysTTL, context.getDeletedKeysTTL(), tableName, _partitionId);
      return false;
    }

    // Check upsert mode and partial upsert strategies
    TableConfig tableConfig = context.getTableConfig();
    if (tableConfig != null) {
      UpsertConfig.Mode currentUpsertMode = tableConfig.getUpsertMode();
      if (_upsertMode != null && _upsertMode != currentUpsertMode) {
        LOGGER.info("Previous snapshot used upsert mode: {} different from current: {} for table: {}, partition: {}",
            _upsertMode, currentUpsertMode, tableName, _partitionId);
        return false;
      }

      if (currentUpsertMode == UpsertConfig.Mode.PARTIAL) {
        UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
        if (upsertConfig != null) {
          if (_defaultPartialUpsertStrategy != null
              && _defaultPartialUpsertStrategy != upsertConfig.getDefaultPartialUpsertStrategy()) {
            LOGGER.info("Previous snapshot used default partial strategy: {} different from current: {} for table: "
                + "{}, partition: {}", _defaultPartialUpsertStrategy, upsertConfig.getDefaultPartialUpsertStrategy(),
                tableName, _partitionId);
            return false;
          }

          if (_partialUpsertStrategies != null
              && !_partialUpsertStrategies.equals(upsertConfig.getPartialUpsertStrategies())) {
            LOGGER.info("Previous snapshot used partial upsert strategies: {} different from current: {} for table: "
                + "{}, partition: {}", _partialUpsertStrategies, upsertConfig.getPartialUpsertStrategies(),
                tableName, _partitionId);
            return false;
          }
        }
      }
    }

    LOGGER.debug("Snapshot metadata is compatible for table: {}, partition: {}", tableName, _partitionId);
    return true;
  }

  // Getters and setters

  public int getVersion() {
    return _version;
  }

  public void setVersion(int version) {
    _version = version;
  }

  public int getPartitionId() {
    return _partitionId;
  }

  public void setPartitionId(int partitionId) {
    _partitionId = partitionId;
  }

  public long getCreationTime() {
    return _creationTime;
  }

  public void setCreationTime(long creationTime) {
    _creationTime = creationTime;
  }

  public List<String> getPrimaryKeyColumns() {
    return _primaryKeyColumns;
  }

  public void setPrimaryKeyColumns(List<String> primaryKeyColumns) {
    _primaryKeyColumns = primaryKeyColumns;
  }

  public List<String> getComparisonColumns() {
    return _comparisonColumns;
  }

  public void setComparisonColumns(List<String> comparisonColumns) {
    _comparisonColumns = comparisonColumns;
  }

  public String getDeleteRecordColumn() {
    return _deleteRecordColumn;
  }

  public void setDeleteRecordColumn(String deleteRecordColumn) {
    _deleteRecordColumn = deleteRecordColumn;
  }

  public HashFunction getHashFunction() {
    return _hashFunction;
  }

  public void setHashFunction(HashFunction hashFunction) {
    _hashFunction = hashFunction;
  }

  public double getMetadataTTL() {
    return _metadataTTL;
  }

  public void setMetadataTTL(double metadataTTL) {
    _metadataTTL = metadataTTL;
  }

  public double getDeletedKeysTTL() {
    return _deletedKeysTTL;
  }

  public void setDeletedKeysTTL(double deletedKeysTTL) {
    _deletedKeysTTL = deletedKeysTTL;
  }

  public UpsertConfig.Mode getUpsertMode() {
    return _upsertMode;
  }

  public void setUpsertMode(UpsertConfig.Mode upsertMode) {
    _upsertMode = upsertMode;
  }

  public Map<String, UpsertConfig.Strategy> getPartialUpsertStrategies() {
    return _partialUpsertStrategies;
  }

  public void setPartialUpsertStrategies(Map<String, UpsertConfig.Strategy> partialUpsertStrategies) {
    _partialUpsertStrategies = partialUpsertStrategies;
  }

  public UpsertConfig.Strategy getDefaultPartialUpsertStrategy() {
    return _defaultPartialUpsertStrategy;
  }

  public void setDefaultPartialUpsertStrategy(UpsertConfig.Strategy defaultPartialUpsertStrategy) {
    _defaultPartialUpsertStrategy = defaultPartialUpsertStrategy;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LocalValidDocIdsSnapshotMetadata that = (LocalValidDocIdsSnapshotMetadata) o;
    return _version == that._version && _partitionId == that._partitionId && _creationTime == that._creationTime
        && Double.compare(that._metadataTTL, _metadataTTL) == 0
        && Double.compare(that._deletedKeysTTL, _deletedKeysTTL) == 0
        && Objects.equals(_primaryKeyColumns, that._primaryKeyColumns)
        && Objects.equals(_comparisonColumns, that._comparisonColumns)
        && Objects.equals(_deleteRecordColumn, that._deleteRecordColumn)
        && _hashFunction == that._hashFunction
        && _upsertMode == that._upsertMode
        && Objects.equals(_partialUpsertStrategies, that._partialUpsertStrategies)
        && _defaultPartialUpsertStrategy == that._defaultPartialUpsertStrategy;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_version, _partitionId, _creationTime, _primaryKeyColumns, _comparisonColumns,
        _deleteRecordColumn, _hashFunction, _metadataTTL, _deletedKeysTTL, _upsertMode, _partialUpsertStrategies,
        _defaultPartialUpsertStrategy);
  }

  @Override
  public String toString() {
    return "LocalValidDocIdsSnapshotMetadata{"
        + "_version=" + _version
        + ", _partitionId=" + _partitionId
        + ", _creationTime=" + _creationTime
        + ", _primaryKeyColumns=" + _primaryKeyColumns
        + ", _comparisonColumns=" + _comparisonColumns
        + ", _deleteRecordColumn='" + _deleteRecordColumn + '\''
        + ", _hashFunction=" + _hashFunction
        + ", _metadataTTL=" + _metadataTTL
        + ", _deletedKeysTTL=" + _deletedKeysTTL
        + ", _upsertMode=" + _upsertMode
        + ", _partialUpsertStrategies=" + _partialUpsertStrategies
        + ", _defaultPartialUpsertStrategy=" + _defaultPartialUpsertStrategy
        + '}';
  }
}
