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
package org.apache.pinot.core.operator.combine.merger;

import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;


public class DistinctResultsBlockMerger implements ResultsBlockMerger<DistinctResultsBlock> {

  @Override
  public boolean isQuerySatisfied(DistinctResultsBlock resultsBlock) {
    return resultsBlock.getDistinctTable().isSatisfied();
  }

  @Override
  public void mergeResultsBlocks(DistinctResultsBlock mergedBlock, DistinctResultsBlock blockToMerge) {
    mergedBlock.getDistinctTable().mergeDistinctTable(blockToMerge.getDistinctTable());
    // Propagate lite-cap truncation flag/reason from children to merged block (OR semantics)
    String childLite =
        blockToMerge.getResultsMetadata().get(org.apache.pinot.common.datatable.DataTable.MetadataKey.LITE_LEAF_CAP_TRUNCATION.getName());
    if ("true".equals(childLite)) {
      mergedBlock.setLiteLeafLimitReached(true);
      String reason =
          blockToMerge.getResultsMetadata().get(org.apache.pinot.common.datatable.DataTable.MetadataKey.LEAF_TRUNCATION_REASON.getName());
      if (reason != null) {
        mergedBlock.setLeafTruncationReason(reason);
      }
    }
  }
}
