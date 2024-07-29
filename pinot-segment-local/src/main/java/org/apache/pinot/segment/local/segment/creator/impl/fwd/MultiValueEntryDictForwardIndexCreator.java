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
package org.apache.pinot.segment.local.segment.creator.impl.fwd;

import java.io.File;
import java.io.IOException;
import org.apache.pinot.segment.local.io.util.PinotDataBitSet;
import org.apache.pinot.segment.local.io.writer.impl.FixedBitMVEntryDictForwardIndexWriter;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Forward index creator for dictionary-encoded multi-value column, where multi-value entries are dictionary encoded.
 */
public class MultiValueEntryDictForwardIndexCreator implements ForwardIndexCreator {
  private final FixedBitMVEntryDictForwardIndexWriter _writer;

  public MultiValueEntryDictForwardIndexCreator(File outputDir, String column, int cardinality, int numDocs) {
    File indexFile = new File(outputDir, column + V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION);
    int numBitsPerValue = PinotDataBitSet.getNumBitsPerValue(cardinality - 1);
    _writer = new FixedBitMVEntryDictForwardIndexWriter(indexFile, numDocs, numBitsPerValue);
  }

  @Override
  public boolean isDictionaryEncoded() {
    return true;
  }

  @Override
  public boolean isSingleValue() {
    return false;
  }

  @Override
  public DataType getValueType() {
    return DataType.INT;
  }

  @Override
  public void putDictIdMV(int[] dictIds) {
    _writer.putDictIds(dictIds);
  }

  @Override
  public void close()
      throws IOException {
    _writer.close();
  }
}
