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
package org.apache.pinot.segment.local.segment.index.readers.forward;

import java.io.IOException;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * No-op forward index reader meant to be used by single-value columns with forward index disabled. This is meant to
 * provide clean exception handling for operations which need to read forward index when forward index is disabled.
 */
public class NoOpSVForwardIndexReader implements ForwardIndexReader<ForwardIndexReaderContext> {
  @Override
  public boolean isDictionaryEncoded() {
    return true;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public FieldSpec.DataType getStoredType() {
    return FieldSpec.DataType.INT;
  }

  @Override
  public int getDictId(int docId, ForwardIndexReaderContext context) {
    throw new UnsupportedOperationException("Forward index is disabled, cannot read dictId for docId");
  }

  @Override
  public void readDictIds(int[] docIds, int length, int[] dictIdBuffer, ForwardIndexReaderContext context) {
    throw new UnsupportedOperationException("Forward index is disabled, cannot read dictIds for docId list");
  }

  @Override
  public void close()
      throws IOException {
  }
}
