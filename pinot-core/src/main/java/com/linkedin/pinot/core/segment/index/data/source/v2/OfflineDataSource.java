/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.segment.index.data.source.v2;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.segment.index.column.ColumnIndexContainer;


public class OfflineDataSource implements DataSource {

  private final String column;
  private final ColumnIndexContainer container;

  public OfflineDataSource(String column, ColumnIndexContainer indexContainer) {
    this.column = column;
    this.container = indexContainer;
  }

  @Override
  public boolean open() {
    return true;
  }

  @Override
  public Block nextBlock() {
    return null;
  }

  @Override
  public Block nextBlock(BlockId BlockId) {
    return null;
  }

  @Override
  public boolean close() {
    return true;
  }

  @Override
  public boolean setPredicate(Predicate predicate) {
    throw new UnsupportedOperationException("cannot set predicate on this data source");
  }

  @Override
  public DataSourceMetadata getDataSourceMetadata() {
    return new DataSourceMetadata() {

      @Override
      public boolean isSorted() {
        return container.getColumnMetadata().isSorted();
      }

      @Override
      public boolean isSingleValue() {
        return container.getColumnMetadata().isSingleValue();
      }

      @Override
      public boolean hasInvertedIndex() {
        return (container.getColumnMetadata().isHasInvertedIndex() && container.getInvertedIndex() != null);
      }

      @Override
      public boolean hasDictionary() {
        return container.getColumnMetadata().hasDictionary();
      }

      @Override
      public FieldType getFieldType() {
        return container.getColumnMetadata().getFieldType();
      }

      @Override
      public DataType getDataType() {
        return container.getColumnMetadata().getDataType();
      }

      @Override
      public int cardinality() {
        return container.getColumnMetadata().getCardinality();
      }
    };
  }

}
