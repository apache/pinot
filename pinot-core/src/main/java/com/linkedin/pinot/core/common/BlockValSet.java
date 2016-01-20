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
package com.linkedin.pinot.core.common;

import com.linkedin.pinot.common.data.FieldSpec.DataType;

/**
 *
 *
 */
public interface BlockValSet {

  BlockValIterator iterator();

  DataType getValueType();

  /**
   * Copies the dictionaryIds for the input range DocIds.
   * Expects that the out array is properly sized
   * @param inDocIds input set of doc ids for which to read dictionaryIds
   * @param inStartPos start index in inDocIds
   * @param inDocIdsSize size of inDocIds
   * @param outDictionaryIds out parameter giving the dictionary ids corresponding to
   *                         input docIds
   * @param outStartPos starting index position in outDictionaryIds. Indexes will
   *                    be copied starting at this position.
   *                    outDictionaryIds must be atleast (outStartPos + inDocIdsSize) in size
   */
  void readIntValues(int[] inDocIds, int inStartPos, int inDocIdsSize, int[] outDictionaryIds, int outStartPos);

}
