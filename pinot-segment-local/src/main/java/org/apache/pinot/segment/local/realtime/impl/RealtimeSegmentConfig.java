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
package org.apache.pinot.segment.local.realtime.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.spi.index.creator.H3IndexConfig;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;


public class RealtimeSegmentConfig {
  private final String _tableNameWithType;
  private final String _segmentName;
  private final String _streamName;
  private final Schema _schema;
  private final String _timeColumnName;
  private final int _capacity;
  private final int _avgNumMultiValues;
  private final Set<String> _noDictionaryColumns;
  private final Set<String> _varLengthDictionaryColumns;
  private final Set<String> _invertedIndexColumns;
  private final Set<String> _textIndexColumns;
  private final Set<String> _fstIndexColumns;
  private final Set<String> _jsonIndexColumns;
  private final Map<String, H3IndexConfig> _h3IndexConfigs;
  private final SegmentZKMetadata _segmentZKMetadata;
  private final boolean _offHeap;
  private final PinotDataBufferMemoryManager _memoryManager;
  private final RealtimeSegmentStatsHistory _statsHistory;
  private final String _partitionColumn;
  private final PartitionFunction _partitionFunction;
  private final int _partitionId;
  private final boolean _aggregateMetrics;
  private final boolean _nullHandlingEnabled;
  private final UpsertConfig.Mode _upsertMode;
  private final UpsertConfig.HashFunction _hashFunction;
  private final String _upsertComparisonColumn;
  private final PartitionUpsertMetadataManager _partitionUpsertMetadataManager;
  private final String _consumerDir;

  // TODO: Clean up this constructor. Most of these things can be extracted from tableConfig.
  private RealtimeSegmentConfig(String tableNameWithType, String segmentName, String streamName, Schema schema,
      String timeColumnName, int capacity, int avgNumMultiValues, Set<String> noDictionaryColumns,
      Set<String> varLengthDictionaryColumns, Set<String> invertedIndexColumns, Set<String> textIndexColumns,
      Set<String> fstIndexColumns, Set<String> jsonIndexColumns, Map<String, H3IndexConfig> h3IndexConfigs,
      SegmentZKMetadata segmentZKMetadata, boolean offHeap, PinotDataBufferMemoryManager memoryManager,
      RealtimeSegmentStatsHistory statsHistory, String partitionColumn, PartitionFunction partitionFunction,
      int partitionId, boolean aggregateMetrics, boolean nullHandlingEnabled, String consumerDir,
      UpsertConfig.Mode upsertMode, String upsertComparisonColumn, UpsertConfig.HashFunction hashFunction,
      PartitionUpsertMetadataManager partitionUpsertMetadataManager) {
    _tableNameWithType = tableNameWithType;
    _segmentName = segmentName;
    _streamName = streamName;
    _schema = schema;
    _timeColumnName = timeColumnName;
    _capacity = capacity;
    _avgNumMultiValues = avgNumMultiValues;
    _noDictionaryColumns = noDictionaryColumns;
    _varLengthDictionaryColumns = varLengthDictionaryColumns;
    _invertedIndexColumns = invertedIndexColumns;
    _textIndexColumns = textIndexColumns;
    _fstIndexColumns = fstIndexColumns;
    _jsonIndexColumns = jsonIndexColumns;
    _h3IndexConfigs = h3IndexConfigs;
    _segmentZKMetadata = segmentZKMetadata;
    _offHeap = offHeap;
    _memoryManager = memoryManager;
    _statsHistory = statsHistory;
    _partitionColumn = partitionColumn;
    _partitionFunction = partitionFunction;
    _partitionId = partitionId;
    _aggregateMetrics = aggregateMetrics;
    _nullHandlingEnabled = nullHandlingEnabled;
    _consumerDir = consumerDir;
    _upsertMode = upsertMode != null ? upsertMode : UpsertConfig.Mode.NONE;
    _hashFunction = hashFunction != null ? hashFunction : UpsertConfig.HashFunction.NONE;
    _upsertComparisonColumn = upsertComparisonColumn;
    _partitionUpsertMetadataManager = partitionUpsertMetadataManager;
  }

  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public String getStreamName() {
    return _streamName;
  }

  public Schema getSchema() {
    return _schema;
  }

  public String getTimeColumnName() {
    return _timeColumnName;
  }

  public int getCapacity() {
    return _capacity;
  }

  public int getAvgNumMultiValues() {
    return _avgNumMultiValues;
  }

  public Set<String> getNoDictionaryColumns() {
    return _noDictionaryColumns;
  }

  public Set<String> getVarLengthDictionaryColumns() {
    return _varLengthDictionaryColumns;
  }

  public Set<String> getInvertedIndexColumns() {
    return _invertedIndexColumns;
  }

  /**
   * Used by {@link MutableSegmentImpl} when
   * it starts consuming/indexing.
   * @return
   */
  public Set<String> getTextIndexColumns() {
    return _textIndexColumns;
  }

  public Set<String> getFSTIndexColumns() {
    return _fstIndexColumns;
  }

  public Set<String> getJsonIndexColumns() {
    return _jsonIndexColumns;
  }

  public Map<String, H3IndexConfig> getH3IndexConfigs() {
    return _h3IndexConfigs;
  }

  public SegmentZKMetadata getSegmentZKMetadata() {
    return _segmentZKMetadata;
  }

  public boolean isOffHeap() {
    return _offHeap;
  }

  public PinotDataBufferMemoryManager getMemoryManager() {
    return _memoryManager;
  }

  public RealtimeSegmentStatsHistory getStatsHistory() {
    return _statsHistory;
  }

  public String getPartitionColumn() {
    return _partitionColumn;
  }

  public PartitionFunction getPartitionFunction() {
    return _partitionFunction;
  }

  public int getPartitionId() {
    return _partitionId;
  }

  public boolean aggregateMetrics() {
    return _aggregateMetrics;
  }

  public boolean isNullHandlingEnabled() {
    return _nullHandlingEnabled;
  }

  public String getConsumerDir() {
    return _consumerDir;
  }

  public UpsertConfig.Mode getUpsertMode() {
    return _upsertMode;
  }

  public UpsertConfig.HashFunction getHashFunction() {
    return _hashFunction;
  }

  public String getUpsertComparisonColumn() {
    return _upsertComparisonColumn;
  }

  public PartitionUpsertMetadataManager getPartitionUpsertMetadataManager() {
    return _partitionUpsertMetadataManager;
  }

  public static class Builder {
    private String _tableNameWithType;
    private String _segmentName;
    private String _streamName;
    private Schema _schema;
    private String _timeColumnName;
    private int _capacity;
    private int _avgNumMultiValues;
    private Set<String> _noDictionaryColumns;
    private Set<String> _varLengthDictionaryColumns;
    private Set<String> _invertedIndexColumns;
    private Set<String> _textIndexColumns = new HashSet<>();
    private Set<String> _fstIndexColumns = new HashSet<>();
    private Set<String> _jsonIndexColumns = new HashSet<>();
    private Map<String, H3IndexConfig> _h3IndexConfigs = new HashMap<>();
    private SegmentZKMetadata _segmentZKMetadata;
    private boolean _offHeap;
    private PinotDataBufferMemoryManager _memoryManager;
    private RealtimeSegmentStatsHistory _statsHistory;
    private String _partitionColumn;
    private PartitionFunction _partitionFunction;
    private int _partitionId;
    private boolean _aggregateMetrics = false;
    private boolean _nullHandlingEnabled = false;
    private String _consumerDir;
    private UpsertConfig.Mode _upsertMode;
    private UpsertConfig.HashFunction _hashFunction;
    private String _upsertComparisonColumn;
    private PartitionUpsertMetadataManager _partitionUpsertMetadataManager;

    public Builder() {
    }

    public Builder setTableNameWithType(String tableNameWithType) {
      _tableNameWithType = tableNameWithType;
      return this;
    }

    public Builder setSegmentName(String segmentName) {
      _segmentName = segmentName;
      return this;
    }

    public Builder setStreamName(String streamName) {
      _streamName = streamName;
      return this;
    }

    public Builder setSchema(Schema schema) {
      _schema = schema;
      return this;
    }

    public Builder setTimeColumnName(String timeColumnName) {
      _timeColumnName = timeColumnName;
      return this;
    }

    public Builder setCapacity(int capacity) {
      _capacity = capacity;
      return this;
    }

    public Builder setAvgNumMultiValues(int avgNumMultiValues) {
      _avgNumMultiValues = avgNumMultiValues;
      return this;
    }

    public Builder setNoDictionaryColumns(Set<String> noDictionaryColumns) {
      _noDictionaryColumns = noDictionaryColumns;
      return this;
    }

    public Builder setVarLengthDictionaryColumns(Set<String> varLengthDictionaryColumns) {
      _varLengthDictionaryColumns = varLengthDictionaryColumns;
      return this;
    }

    public Builder setInvertedIndexColumns(Set<String> invertedIndexColumns) {
      _invertedIndexColumns = invertedIndexColumns;
      return this;
    }

    /**
     * Used by LLRealtimeSegmentDataManager
     * to set the list of text index creation columns. This list is later used by
     * {@link MutableSegmentImpl} when
     * it starts consuming/indexing.
     * @param textIndexColumns set of text index enabled columns
     * @return builder
     */
    public Builder setTextIndexColumns(Set<String> textIndexColumns) {
      _textIndexColumns = textIndexColumns;
      return this;
    }

    public Builder setFSTIndexColumns(Set<String> fstIndexColumns) {
      _fstIndexColumns = fstIndexColumns;
      return this;
    }

    public Builder setJsonIndexColumns(Set<String> jsonIndexColumns) {
      _jsonIndexColumns = jsonIndexColumns;
      return this;
    }

    public Builder setH3IndexConfigs(Map<String, H3IndexConfig> h3IndexConfigs) {
      _h3IndexConfigs = h3IndexConfigs;
      return this;
    }

    public Builder setSegmentZKMetadata(SegmentZKMetadata segmentZKMetadata) {
      _segmentZKMetadata = segmentZKMetadata;
      return this;
    }

    public Builder setOffHeap(boolean offHeap) {
      _offHeap = offHeap;
      return this;
    }

    public Builder setMemoryManager(PinotDataBufferMemoryManager memoryManager) {
      _memoryManager = memoryManager;
      return this;
    }

    public Builder setStatsHistory(RealtimeSegmentStatsHistory statsHistory) {
      _statsHistory = statsHistory;
      return this;
    }

    public Builder setPartitionColumn(String partitionColumn) {
      _partitionColumn = partitionColumn;
      return this;
    }

    public Builder setPartitionFunction(PartitionFunction partitionFunction) {
      _partitionFunction = partitionFunction;
      return this;
    }

    public Builder setPartitionId(int partitionId) {
      _partitionId = partitionId;
      return this;
    }

    public Builder setAggregateMetrics(boolean aggregateMetrics) {
      _aggregateMetrics = aggregateMetrics;
      return this;
    }

    public Builder setNullHandlingEnabled(boolean nullHandlingEnabled) {
      _nullHandlingEnabled = nullHandlingEnabled;
      return this;
    }

    public Builder setConsumerDir(String consumerDir) {
      _consumerDir = consumerDir;
      return this;
    }

    public Builder setUpsertMode(UpsertConfig.Mode upsertMode) {
      _upsertMode = upsertMode;
      return this;
    }

    public Builder setHashFunction(UpsertConfig.HashFunction hashFunction) {
      _hashFunction = hashFunction;
      return this;
    }

    public Builder setUpsertComparisonColumn(String upsertComparisonColumn) {
      _upsertComparisonColumn = upsertComparisonColumn;
      return this;
    }

    public Builder setPartitionUpsertMetadataManager(PartitionUpsertMetadataManager partitionUpsertMetadataManager) {
      _partitionUpsertMetadataManager = partitionUpsertMetadataManager;
      return this;
    }

    public RealtimeSegmentConfig build() {
      return new RealtimeSegmentConfig(_tableNameWithType, _segmentName, _streamName, _schema, _timeColumnName,
          _capacity, _avgNumMultiValues, _noDictionaryColumns, _varLengthDictionaryColumns, _invertedIndexColumns,
          _textIndexColumns, _fstIndexColumns, _jsonIndexColumns, _h3IndexConfigs, _segmentZKMetadata, _offHeap,
          _memoryManager, _statsHistory, _partitionColumn, _partitionFunction, _partitionId, _aggregateMetrics,
          _nullHandlingEnabled, _consumerDir, _upsertMode, _upsertComparisonColumn, _hashFunction,
          _partitionUpsertMetadataManager);
    }
  }
}
