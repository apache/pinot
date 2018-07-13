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
package com.linkedin.pinot.core.operator.query;

import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.utils.DataSchema;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.ExecutionStatistics;
import com.linkedin.pinot.core.operator.blocks.IntermediateResultsBlock;
import com.linkedin.pinot.core.query.selection.SelectionOperatorUtils;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;


/**
 * The <code>EmptySelectionOperator</code> class provides the operator for selection query with LIMIT less or equal to 0
 * on a single segment.
 * <p>NOTE: this operator short circuit underlying operators and directly returns the data schema without any rows.
 */
public class EmptySelectionOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final String OPERATOR_NAME = "EmptySelectionOperator";

  private final DataSchema _dataSchema;
  private final ExecutionStatistics _executionStatistics;

  public EmptySelectionOperator(IndexSegment indexSegment, Selection selection) {
    List<String> selectionColumns =
        SelectionOperatorUtils.getSelectionColumns(selection.getSelectionColumns(), indexSegment);
    _dataSchema = SelectionOperatorUtils.extractDataSchema(null, selectionColumns, indexSegment);
    _executionStatistics = new ExecutionStatistics(0L, 0L, 0L, indexSegment.getSegmentMetadata().getTotalRawDocs());
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    return new IntermediateResultsBlock(_dataSchema, Collections.<Serializable[]>emptyList());
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return _executionStatistics;
  }
}
