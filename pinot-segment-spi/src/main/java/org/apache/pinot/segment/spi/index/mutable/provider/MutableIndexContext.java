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
package org.apache.pinot.segment.spi.index.mutable.provider;

import java.io.File;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec;


public class MutableIndexContext {
  private final int _capacity;
  private final FieldSpec _fieldSpec;
  private final boolean _hasDictionary;
  private final boolean _offHeap;
  private final int _estimatedColSize;
  private final int _estimatedCardinality;
  private final int _avgNumMultiValues;
  private final String _segmentName;
  private final PinotDataBufferMemoryManager _memoryManager;
  private final File _consumerDir;

  public MutableIndexContext(FieldSpec fieldSpec, boolean hasDictionary, String segmentName,
      PinotDataBufferMemoryManager memoryManager, int capacity, boolean offHeap, int estimatedColSize,
      int estimatedCardinality, int avgNumMultiValues, File consumerDir) {
    _fieldSpec = fieldSpec;
    _hasDictionary = hasDictionary;
    _segmentName = segmentName;
    _memoryManager = memoryManager;
    _capacity = capacity;
    _offHeap = offHeap;
    _estimatedColSize = estimatedColSize;
    _estimatedCardinality = estimatedCardinality;
    _avgNumMultiValues = avgNumMultiValues;
    _consumerDir = consumerDir;
  }

  public PinotDataBufferMemoryManager getMemoryManager() {
    return _memoryManager;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public FieldSpec getFieldSpec() {
    return _fieldSpec;
  }

  public boolean hasDictionary() {
    return _hasDictionary;
  }

  public int getCapacity() {
    return _capacity;
  }

  public boolean isOffHeap() {
    return _offHeap;
  }

  public int getEstimatedColSize() {
    return _estimatedColSize;
  }

  public int getEstimatedCardinality() {
    return _estimatedCardinality;
  }

  public int getAvgNumMultiValues() {
    return _avgNumMultiValues;
  }

  @Nullable
  public File getConsumerDir() {
    return _consumerDir;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private FieldSpec _fieldSpec;
    private String _segmentName;
    private boolean _hasDictionary = true;
    private boolean _offHeap = true;
    private int _capacity;
    private PinotDataBufferMemoryManager _memoryManager;
    private int _estimatedColSize;
    private int _estimatedCardinality;
    private int _avgNumMultiValues;
    @Nullable
    private File _consumerDir;

    public Builder withMemoryManager(PinotDataBufferMemoryManager memoryManager) {
      _memoryManager = memoryManager;
      return this;
    }

    public Builder withFieldSpec(FieldSpec fieldSpec) {
      _fieldSpec = fieldSpec;
      return this;
    }

    public Builder withSegmentName(String segmentName) {
      _segmentName = segmentName;
      return this;
    }

    public Builder withDictionary(boolean hasDictionary) {
      _hasDictionary = hasDictionary;
      return this;
    }

    public Builder offHeap(boolean offHeap) {
      _offHeap = offHeap;
      return this;
    }

    public Builder withCapacity(int capacity) {
      _capacity = capacity;
      return this;
    }

    public Builder withEstimatedColSize(int estimatedColSize) {
      _estimatedColSize = estimatedColSize;
      return this;
    }

    public Builder withEstimatedCardinality(int estimatedCardinality) {
      _estimatedCardinality = estimatedCardinality;
      return this;
    }

    public Builder withAvgNumMultiValues(int avgNumMultiValues) {
      _avgNumMultiValues = avgNumMultiValues;
      return this;
    }

    public Builder withConsumerDir(File consumerDir) {
      _consumerDir = consumerDir;
      return this;
    }

    public MutableIndexContext build() {
      return new MutableIndexContext(Objects.requireNonNull(_fieldSpec), _hasDictionary,
          Objects.requireNonNull(_segmentName), Objects.requireNonNull(_memoryManager), _capacity, _offHeap,
          _estimatedColSize, _estimatedCardinality, _avgNumMultiValues, _consumerDir);
    }
  }
}
