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
package com.linkedin.pinot.core.operator.filter;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.operator.docidsets.BitmapDocIdSet;
import com.linkedin.pinot.core.operator.docidsets.ScanBasedMultiValueDocIdSet;
import com.linkedin.pinot.core.operator.docidsets.ScanBasedSingleValueDocIdSet;
import com.linkedin.pinot.core.operator.docidsets.SortedDocIdSet;


/**
 * Result Block shared across all operators in the Filter Tree
 * @author kgopalak
 *
 */
public class FilterResultBlock implements Block {

  List<BlockDocIdSet> andBlockDocIdSets = new ArrayList<BlockDocIdSet>();
  List<BlockDocIdSet> orBlockDocIdSets = new ArrayList<BlockDocIdSet>();
  boolean[] result;
  private boolean init;

  public FilterResultBlock(int minDocId, int maxDocId) {
    result = new boolean[maxDocId - minDocId];
  }

  /**
   * sets the docId from min to max in the result block
   * @param min
   * @param max
   */
  private void setRange(int minDocId, int maxDocId) {

  }

  /**
   * unsets the docId's from min to max in the result block
   * @param minDocId
   * @param maxDocId
   */
  private void unsetRange(int minDocId, int maxDocId) {

  }

  /**
   * Includes the docId in the result set
   * @param docId
   */
  private void set(int docId) {

  }

  /**
   * Excludes the docId from the result set
   * @param docId
   */
  private void unset(int docId) {

  }

  /**
   * Check if the docId is included in the result set
   * @param index
   * @return
   */
  private boolean isSet(int docId) {
    return true;
  }

  @Override
  public BlockId getId() {
    return null;
  }

  @Override
  public boolean applyPredicate(Predicate predicate) {
    return true;
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    return null;
  }

  @Override
  public BlockValSet getBlockValueSet() {
    throw new UnsupportedOperationException("Filter Result Block does not support BlockValue Set access.");
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    throw new UnsupportedOperationException("Filter Result Block does not support BlockValue Set access.");
  }

  @Override
  public BlockMetadata getMetadata() {
    return null;
  }

  private void init(BlockDocIdSet blockDocIdSet) {
    init = true;
  }

  public void createAndNode(){
    
  }

  public void createOrNode(){
    
  }

  public void add(BlockDocIdSet blockDocIdSet) {
    if (!init) {
      init(blockDocIdSet);
    } else {
      //TODO:we can do this based on index/operator metadata 
      if (blockDocIdSet instanceof SortedDocIdSet) {

      }
      if (blockDocIdSet instanceof BitmapDocIdSet) {

      }
      if (blockDocIdSet instanceof ScanBasedSingleValueDocIdSet) {

      }
      if (blockDocIdSet instanceof ScanBasedMultiValueDocIdSet) {

      }
    }
  }

  public void or(BlockDocIdSet blockDocIdSet) {

  }

}
