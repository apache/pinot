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

package org.apache.pinot.segment.local.segment.index.inverted;

import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexType;


public class InvertedIndexType
    implements IndexType<Object, IndexReader, IndexCreator> {
  public static final InvertedIndexType INSTANCE = new InvertedIndexType();

  private InvertedIndexType() {
  }

  @Override
  public String getId() {
    return "inverted_index";
  }

  @Override
  public String getFileExtension(ColumnMetadata columnMetadata) {
    return V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION;
  }

  @Override
  public String toString() {
    return getId();
  }
}
