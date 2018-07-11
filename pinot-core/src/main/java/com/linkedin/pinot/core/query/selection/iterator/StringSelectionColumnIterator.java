/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.query.selection.iterator;

import com.clearspring.analytics.util.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.utils.primitive.ByteArray;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import java.io.Serializable;


/**
 * Iterator on double no dictionary column selection query.
 *
 */
public class StringSelectionColumnIterator implements SelectionColumnIterator {
  private final FieldSpec.DataType _dataType;
  protected BlockSingleValIterator bvIter;

  public StringSelectionColumnIterator(Block block) {
    _dataType = block.getMetadata().getDataType();
    Preconditions.checkArgument(
        _dataType.equals(FieldSpec.DataType.STRING) || _dataType.equals(FieldSpec.DataType.BYTES),
        "Illegal data type for StringSelectionColumnIterator: " + _dataType);
    bvIter = (BlockSingleValIterator) block.getBlockValueSet().iterator();
  }

  @Override
  public Serializable getValue(int docId) {
    bvIter.skipTo(docId);

    if (_dataType.equals(FieldSpec.DataType.BYTES)) {
      // byte[] is converted to equivalent Hex String for selection queries.
      byte[] bytes = bvIter.nextBytesVal();
      return ByteArray.toHexString(bytes);
    } else {
      return bvIter.nextStringVal();
    }
  }
}
