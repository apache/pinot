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
package org.apache.pinot.segment.local.segment.creator.impl;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.nullvalue.NullValueVectorCreator;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Holds all the index creators and metadata for a single column during segment creation.
 * This is used by ColumnarSegmentCreator to avoid hashmap lookups when processing columns.
 */
public class ColumnIndexCreators implements Closeable {
  private final String _columnName;
  private final FieldSpec _fieldSpec;
  private final SegmentDictionaryCreator _dictionaryCreator; // null for raw columns
  // Indexes whose build lifecycle is not DURING_SEGMENT_CREATION are not included
  private final List<IndexCreator> _indexCreators;
  private final NullValueVectorCreator _nullValueVectorCreator; // null if column is not nullable
  private final boolean _isDictionaryEncoded;
  private volatile boolean _isSealed = false;
  private volatile boolean _isClosed = false;

  public ColumnIndexCreators(String columnName, FieldSpec fieldSpec,
      @Nullable SegmentDictionaryCreator dictionaryCreator,
      List<IndexCreator> indexCreators,
      @Nullable NullValueVectorCreator nullValueVectorCreator) {
    _columnName = columnName;
    _fieldSpec = fieldSpec;
    _dictionaryCreator = dictionaryCreator;
    _indexCreators = indexCreators;
    _nullValueVectorCreator = nullValueVectorCreator;
    _isDictionaryEncoded = dictionaryCreator != null;
  }

  public String getColumnName() {
    return _columnName;
  }

  public FieldSpec getFieldSpec() {
    return _fieldSpec;
  }

  @Nullable
  public SegmentDictionaryCreator getDictionaryCreator() {
    return _dictionaryCreator;
  }

  public List<IndexCreator> getIndexCreators() {
    return _indexCreators;
  }

  @Nullable
  public NullValueVectorCreator getNullValueVectorCreator() {
    return _nullValueVectorCreator;
  }

  public boolean isDictionaryEncoded() {
    return _isDictionaryEncoded;
  }

  public void seal() throws IOException {
    if (_isSealed) {
      return;
    }
    // Set isSealed before sealing the creators to avoid sealing again if any exception is thrown during seal
    _isSealed = true;
    if (_dictionaryCreator != null) {
      _dictionaryCreator.seal();
    }
    if (_nullValueVectorCreator != null) {
      _nullValueVectorCreator.seal();
    }
    for (IndexCreator indexCreator : _indexCreators) {
      indexCreator.seal();
    }
  }

  @Override
  public void close()
      throws IOException {
    if (_isClosed) {
      return;
    }
    // Set isClosed before closing the creators to avoid closing again if any exception is thrown during close
    _isClosed = true;
    List<Closeable> creators = new ArrayList<>();
    if (_dictionaryCreator != null) {
      creators.add(_dictionaryCreator);
    }
    if (_nullValueVectorCreator != null) {
      creators.add(_nullValueVectorCreator);
    }
    creators.addAll(_indexCreators);
    FileUtils.close(creators);
  }
}
