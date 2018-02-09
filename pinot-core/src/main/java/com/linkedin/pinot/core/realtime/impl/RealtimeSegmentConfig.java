/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.realtime.impl;

import com.linkedin.pinot.common.config.SegmentPartitionConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import java.util.Set;


public class RealtimeSegmentConfig {
  private final String _segmentName;
  private final String _streamName;
  private final Schema _schema;
  private final int _capacity;
  private final int _avgNumMultiValues;
  private final Set<String> _noDictionaryColumns;
  private final Set<String> _invertedIndexColumns;
  private final RealtimeSegmentZKMetadata _realtimeSegmentZKMetadata;
  private final boolean _offHeap;
  private final PinotDataBufferMemoryManager _memoryManager;
  private final RealtimeSegmentStatsHistory _statsHistory;
  private final SegmentPartitionConfig _segmentPartitionConfig;

  private RealtimeSegmentConfig(String segmentName, String streamName, Schema schema, int capacity,
      int avgNumMultiValues, Set<String> noDictionaryColumns, Set<String> invertedIndexColumns,
      RealtimeSegmentZKMetadata realtimeSegmentZKMetadata, boolean offHeap,
      PinotDataBufferMemoryManager memoryManager, RealtimeSegmentStatsHistory statsHistory,
      SegmentPartitionConfig segmentPartitionConfig) {
    _segmentName = segmentName;
    _streamName = streamName;
    _schema = schema;
    _capacity = capacity;
    _avgNumMultiValues = avgNumMultiValues;
    _noDictionaryColumns = noDictionaryColumns;
    _invertedIndexColumns = invertedIndexColumns;
    _realtimeSegmentZKMetadata = realtimeSegmentZKMetadata;
    _offHeap = offHeap;
    _memoryManager = memoryManager;
    _statsHistory = statsHistory;
    _segmentPartitionConfig = segmentPartitionConfig;
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

  public int getCapacity() {
    return _capacity;
  }

  public int getAvgNumMultiValues() {
    return _avgNumMultiValues;
  }

  public Set<String> getNoDictionaryColumns() {
    return _noDictionaryColumns;
  }

  public Set<String> getInvertedIndexColumns() {
    return _invertedIndexColumns;
  }

  public RealtimeSegmentZKMetadata getRealtimeSegmentZKMetadata() {
    return _realtimeSegmentZKMetadata;
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

  public SegmentPartitionConfig getSegmentPartitionConfig() {
    return _segmentPartitionConfig;
  }

  public static class Builder {
    private String _segmentName;
    private String _streamName;
    private Schema _schema;
    private int _capacity;
    private int _avgNumMultiValues;
    private Set<String> _noDictionaryColumns;
    private Set<String> _invertedIndexColumns;
    private RealtimeSegmentZKMetadata _realtimeSegmentZKMetadata;
    private boolean _offHeap;
    private PinotDataBufferMemoryManager _memoryManager;
    private RealtimeSegmentStatsHistory _statsHistory;
    private SegmentPartitionConfig _segmentPartitionConfig;

    public Builder() {
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

    public Builder setInvertedIndexColumns(Set<String> invertedIndexColumns) {
      _invertedIndexColumns = invertedIndexColumns;
      return this;
    }

    public Builder setRealtimeSegmentZKMetadata(RealtimeSegmentZKMetadata realtimeSegmentZKMetadata) {
      _realtimeSegmentZKMetadata = realtimeSegmentZKMetadata;
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

    public Builder setSegmentPartitionConfig(SegmentPartitionConfig segmentPartitionConfig) {
      _segmentPartitionConfig = segmentPartitionConfig;
      return this;
    }

    public RealtimeSegmentConfig build() {
      return new RealtimeSegmentConfig(_segmentName, _streamName, _schema, _capacity, _avgNumMultiValues,
          _noDictionaryColumns, _invertedIndexColumns, _realtimeSegmentZKMetadata, _offHeap, _memoryManager,
          _statsHistory, _segmentPartitionConfig);
    }
  }
}
