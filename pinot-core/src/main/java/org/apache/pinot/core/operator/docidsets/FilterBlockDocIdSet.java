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
package org.apache.pinot.core.operator.docidsets;

import org.apache.pinot.core.common.BlockDocIdSet;


/**
 * The <code>FilterBlockDocIdSet</code> interface represents the <code>BlockDocIdSet</code> returned by
 * <code>BaseFilterBlock</code>.
 *
 * <p>To accelerate the filter process, we added several methods to help filtering out documents that do not need to be
 * processed.
 *
 * <p>The correct order of calling these methods are:
 * <ul>
 *   <li>
 *     Construct the <code>FilterBlockDocIdSet</code>
 *   </li>
 *   <li>
 *     Call <code>getMinDocId()</code> and <code>getMaxDocId()</code> to gather information
 *   </li>
 *   <li>
 *     Narrow down the documents that need to be processed by calling <code>setStartDocId()</code> and
 *     <code>setEndDocId()</code>
 *   </li>
 *   <li>
 *     Call <code>iterator()</code> to get all documents that are selected
 *   </li>
 * </ul>
 *
 * TODO: Revisit to see whether these methods can accelerate the filtering
 */
public interface FilterBlockDocIdSet extends BlockDocIdSet {

  /**
   * Returns the minimum document id the set can possibly contain.
   */
  int getMinDocId();

  /**
   * Returns the maximum document id (inclusive) the set can possibly contain.
   * TODO: Change it to exclusive
   */
  int getMaxDocId();

  /**
   * Sets the start document id that need to be processed.
   */
  void setStartDocId(int startDocId);

  /**
   * Sets the end document id (inclusive) that need to be processed.
   * TODO: Change it to exclusive
   */
  void setEndDocId(int endDocId);

  /**
   * Returns the number of entries scanned in filtering phase.
   */
  long getNumEntriesScannedInFilter();
}
