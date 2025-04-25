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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryErrorMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SelectionOnlyResultsBlockMerger implements ResultsBlockMerger<SelectionResultsBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SelectionOnlyResultsBlockMerger.class);
  private final int _numRowsToKeep;

  public SelectionOnlyResultsBlockMerger(QueryContext queryContext) {
    _numRowsToKeep = queryContext.getLimit();
  }

  @Override
  public boolean isQuerySatisfied(SelectionResultsBlock resultsBlock) {
    return resultsBlock.getNumRows() >= _numRowsToKeep;
  }

  @Override
  public void mergeResultsBlocks(SelectionResultsBlock mergedBlock, SelectionResultsBlock blockToMerge) {
    DataSchema mergedDataSchema = mergedBlock.getDataSchema();
    DataSchema dataSchemaToMerge = blockToMerge.getDataSchema();
    assert mergedDataSchema != null && dataSchemaToMerge != null;
    QueryContext mergedQueryContext = mergedBlock.getQueryContext();
    if (!mergedDataSchema.equals(dataSchemaToMerge) && mergedQueryContext != null
            && mergedQueryContext.isSelectStarQuery()) {
      Map<String, Integer> mergedBlockIndexMap = mergedDataSchema.getColumnNameToIndexMap();
      Map<String, Integer> blockToMergeIndexMap = dataSchemaToMerge.getColumnNameToIndexMap();

      List<String> commonColumns = mergedBlockIndexMap.keySet().stream().filter(blockToMergeIndexMap::containsKey)
              .collect(Collectors.toList());

      if (commonColumns.isEmpty()) {
        String errorMessage = String.format("No common columns to merge between merged block:"
                + " %s and block to merge: %s", mergedDataSchema, dataSchemaToMerge);
        LOGGER.debug(errorMessage);
        mergedBlock.addErrorMessage(QueryErrorMessage.safeMsg(QueryErrorCode.MERGE_RESPONSE, errorMessage));
        return;
      }

      List<Integer> mergedIndices = commonColumns.stream().map(mergedBlockIndexMap::get).collect(Collectors.toList());
      List<Integer> toMergeIndices = commonColumns.stream().map(blockToMergeIndexMap::get).collect(Collectors.toList());

      SelectionOperatorUtils.mergeMatchingColumns(mergedBlock, blockToMerge, _numRowsToKeep, mergedIndices,
              toMergeIndices);
      // Update the data schema of the merged block to only include common columns
      String[] commonColumnNames = commonColumns.toArray(new String[0]);
      DataSchema.ColumnDataType[] commonColumnTypes =
              Arrays.stream(commonColumnNames)
                      .map(name -> mergedDataSchema.getColumnDataType(mergedBlockIndexMap.get(name)))
                      .toArray(DataSchema.ColumnDataType[]::new);
      mergedBlock.setDataSchema(new DataSchema(commonColumnNames, commonColumnTypes));
      return;
    } else if (!mergedDataSchema.equals(dataSchemaToMerge)) {
      String errorMessage = String.format("Data schemas do not match between merged block: %s and block to merge: %s",
              mergedDataSchema, dataSchemaToMerge);
      LOGGER.debug(errorMessage);
      mergedBlock.addErrorMessage(QueryErrorMessage.safeMsg(QueryErrorCode.MERGE_RESPONSE, errorMessage));
      return;
    }
    SelectionOperatorUtils.mergeWithoutOrdering(mergedBlock, blockToMerge, _numRowsToKeep);
  }
}
