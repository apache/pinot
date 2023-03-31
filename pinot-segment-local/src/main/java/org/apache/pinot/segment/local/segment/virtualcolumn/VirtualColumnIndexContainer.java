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
package org.apache.pinot.segment.local.segment.virtualcolumn;

import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;


/**
 * Column index container for virtual columns.
 */
public class VirtualColumnIndexContainer implements ColumnIndexContainer {
  private final ForwardIndexReader<?> _forwardIndex;
  private final InvertedIndexReader<?> _invertedIndex;
  private final Dictionary _dictionary;

  public VirtualColumnIndexContainer(ForwardIndexReader<?> forwardIndex, InvertedIndexReader<?> invertedIndex,
      Dictionary dictionary) {
    _forwardIndex = forwardIndex;
    _invertedIndex = invertedIndex;
    _dictionary = dictionary;
  }

  @Nullable
  @Override
  public <I extends IndexReader, T extends IndexType<?, I, ?>> I getIndex(T indexType) {
    if (indexType.equals(StandardIndexes.forward())) {
      return (I) _forwardIndex;
    }
    if (indexType.equals(StandardIndexes.inverted())) {
      return (I) _invertedIndex;
    }
    if (indexType.equals(StandardIndexes.dictionary())) {
      return (I) _dictionary;
    }
    return null;
  }

  @Override
  public void close()
      throws IOException {
    _forwardIndex.close();
    if (_invertedIndex != null) {
      _invertedIndex.close();
    }
    if (_dictionary != null) {
      _dictionary.close();
    }
  }
}
