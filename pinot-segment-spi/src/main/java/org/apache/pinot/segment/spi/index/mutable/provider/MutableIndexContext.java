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

import java.util.Objects;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.data.FieldSpec;


public interface MutableIndexContext {
  PinotDataBufferMemoryManager getMemoryManager();

  FieldSpec getFieldSpec();

  String getSegmentName();

  boolean hasDictionary();

  int getCapacity();

  boolean isOffHeap();

  static Builder builder() {
    return new Builder();
  }

  class Builder {
    private FieldSpec _fieldSpec;
    private String _segmentName;
    private boolean _hasDictionary = true;
    private boolean _offHeap = true;
    private int _capacity;
    private PinotDataBufferMemoryManager _memoryManager;

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

    public Common build() {
      return new Common(Objects.requireNonNull(_fieldSpec), _hasDictionary, Objects.requireNonNull(_segmentName),
          Objects.requireNonNull(_memoryManager), _capacity, _offHeap);
    }
  }

  final class Common implements MutableIndexContext {
    private final int _capacity;
    private final FieldSpec _fieldSpec;
    private final boolean _hasDictionary;
    private final boolean _offHeap;
    private final String _segmentName;
    private final PinotDataBufferMemoryManager _memoryManager;

    public Common(FieldSpec fieldSpec, boolean hasDictionary, String segmentName,
        PinotDataBufferMemoryManager memoryManager, int capacity, boolean offHeap) {
      _fieldSpec = fieldSpec;
      _hasDictionary = hasDictionary;
      _segmentName = segmentName;
      _memoryManager = memoryManager;
      _capacity = capacity;
      _offHeap = offHeap;
    }

    @Override
    public PinotDataBufferMemoryManager getMemoryManager() {
      return _memoryManager;
    }

    @Override
    public String getSegmentName() {
      return _segmentName;
    }

    @Override
    public FieldSpec getFieldSpec() {
      return _fieldSpec;
    }

    @Override
    public boolean hasDictionary() {
      return _hasDictionary;
    }

    @Override
    public int getCapacity() {
      return _capacity;
    }

    @Override
    public boolean isOffHeap() {
      return _offHeap;
    }

    public Dictionary forDictionary(int estimatedColSize, int estimatedCardinality) {
      return new Dictionary(this, estimatedColSize, estimatedCardinality);
    }

    public Forward forForwardIndex(int avgNumMultiValues) {
      return new Forward(this, avgNumMultiValues);
    }

    public Inverted forInvertedIndex() {
      return new Inverted(this);
    }

    public Json forJsonIndex(JsonIndexConfig jsonIndexConfig) {
      return new Json(this, jsonIndexConfig);
    }

    public Text forTextIndex() {
      return new Text(this);
    }
  }

  class Wrapper implements MutableIndexContext {

    private final MutableIndexContext _wrapped;

    public Wrapper(MutableIndexContext wrapped) {
      _wrapped = wrapped;
    }

    @Override
    public PinotDataBufferMemoryManager getMemoryManager() {
      return _wrapped.getMemoryManager();
    }

    @Override
    public FieldSpec getFieldSpec() {
      return _wrapped.getFieldSpec();
    }

    @Override
    public String getSegmentName() {
      return _wrapped.getSegmentName();
    }

    @Override
    public boolean hasDictionary() {
      return _wrapped.hasDictionary();
    }

    @Override
    public int getCapacity() {
      return _wrapped.getCapacity();
    }

    @Override
    public boolean isOffHeap() {
      return _wrapped.isOffHeap();
    }
  }

  class Dictionary extends Wrapper {

    private final int _estimatedColSize;
    private final int _estimatedCardinality;

    public Dictionary(MutableIndexContext wrapped, int estimatedColSize, int estimatedCardinality) {
      super(wrapped);
      _estimatedColSize = estimatedColSize;
      _estimatedCardinality = estimatedCardinality;
    }

    public int getEstimatedColSize() {
      return _estimatedColSize;
    }

    public int getEstimatedCardinality() {
      return _estimatedCardinality;
    }
  }

  class Forward extends Wrapper {

    private final int _avgNumMultiValues;

    public Forward(MutableIndexContext wrapped, int avgNumMultiValues) {
      super(wrapped);
      _avgNumMultiValues = avgNumMultiValues;
    }

    public int getAvgNumMultiValues() {
      return _avgNumMultiValues;
    }
  }

  class Inverted extends Wrapper {

    public Inverted(MutableIndexContext wrapped) {
      super(wrapped);
    }
  }

  class Json extends Wrapper {
    private final JsonIndexConfig _jsonIndexConfig;

    public Json(MutableIndexContext wrapped, JsonIndexConfig jsonIndexConfig) {
      super(wrapped);
      _jsonIndexConfig = jsonIndexConfig;
    }

    public JsonIndexConfig getJsonIndexConfig() {
      return _jsonIndexConfig;
    }
  }

  class Text extends Wrapper {

    public Text(MutableIndexContext wrapped) {
      super(wrapped);
    }
  }
}
