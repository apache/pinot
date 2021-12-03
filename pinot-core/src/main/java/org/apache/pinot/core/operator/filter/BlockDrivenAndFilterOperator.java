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
package org.apache.pinot.core.operator.filter;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.VisitableOperator;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.dociditerators.ArrayBasedDocIdIterator;
import org.apache.pinot.core.operator.docidsets.AndDocIdSet;
import org.apache.pinot.core.operator.docidsets.ArrayBasedDocIdSet;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import org.apache.pinot.segment.spi.Constants;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

/**
 * Performs an AND operation on top of a Filter Block DocIDSet
 * and a block from the given filter operator.
 */
public class BlockDrivenAndFilterOperator extends BaseFilterOperator
    implements VisitableOperator {
  private static final String OPERATOR_NAME = "BlockDrivenAndFilterOperator";

  private final BaseFilterOperator _filterOperator;
  private FilterBlockDocIdSet _filterBlockDocIdSet;
  private final int _numDocs;

  public BlockDrivenAndFilterOperator(BaseFilterOperator filterOperator, int numDocs) {
    _filterOperator = filterOperator;
    _numDocs = numDocs;
  }

  @Override
  public FilterBlock getNextBlock() {

    if (_filterBlockDocIdSet != null) {
      List<FilterBlockDocIdSet> filterBlockDocIdSets = new ArrayList<>(2);

      filterBlockDocIdSets.add(_filterBlockDocIdSet);
      filterBlockDocIdSets.add(_filterOperator.nextBlock().getBlockDocIdSet());

      return new FilterBlock(new AndDocIdSet(filterBlockDocIdSets));
    }

    return _filterOperator.nextBlock();
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public List<Operator> getChildOperators() {
    return null;
  }

  @Nullable
  @Override
  public String toExplainString() {
    return null;
  }

  @Override
  public <T> void accept(T incomingObject) {

    if (incomingObject == null) {
      return;
    }

    if (!(incomingObject instanceof TransformBlock)) {
      return;
    }
    
    org.apache.pinot.core.operator.blocks.TransformBlock transformBlock =
        (org.apache.pinot.core.operator.blocks.TransformBlock) incomingObject;

    BlockDocIdSet blockDocIdSet = transformBlock.getBlockDocIdSet();

    if (!(blockDocIdSet instanceof ArrayBasedDocIdSet)) {
      throw new IllegalStateException("Non FilterBlockDocIdSet seen in BlockDrivenAndFilterOperator");
    }

    ArrayBasedDocIdSet arrayBasedDocIdSet = (ArrayBasedDocIdSet) blockDocIdSet;

    List<Integer> dataList = new ArrayList<>();

    ArrayBasedDocIdIterator arrayBasedDocIdIterator = arrayBasedDocIdSet.iterator();

    int currentValue = arrayBasedDocIdIterator.next();
    while (currentValue != Constants.EOF) {
      dataList.add(currentValue);
      currentValue = arrayBasedDocIdIterator.next();
    }

    int[] dataArray = dataList.stream().mapToInt(i -> i).toArray();

    ImmutableRoaringBitmap immutableRoaringBitmap = ImmutableRoaringBitmap.bitmapOf(dataArray);

    _filterBlockDocIdSet = new BitmapDocIdSet(immutableRoaringBitmap, _numDocs);
  }
}
