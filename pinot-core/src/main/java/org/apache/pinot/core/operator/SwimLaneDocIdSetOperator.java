package org.apache.pinot.core.operator;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.CombinedFilterBlock;
import org.apache.pinot.core.operator.blocks.DocIdSetBlock;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import org.apache.pinot.core.operator.filter.CombinedFilterOperator;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.Constants;


public class SwimLaneDocIdSetOperator extends BaseOperator<DocIdSetBlock> {
  private static final String OPERATOR_NAME = "SwimLaneDocIdSetOperator";

  private final CombinedFilterOperator _filterOperator;
  private final ExpressionContext _expressionContext;
  private final int _maxSizeOfDocIdSet;

  private FilterBlockDocIdSet _filterBlockDocIdSet;
  private BlockDocIdIterator _blockDocIdIterator;
  private int _currentDocId = 0;

  public SwimLaneDocIdSetOperator(CombinedFilterOperator filterOperator, ExpressionContext expressionContext,
      int maxSizeOfDocIdSet) {
    Preconditions.checkArgument(maxSizeOfDocIdSet > 0 && maxSizeOfDocIdSet <= DocIdSetPlanNode.MAX_DOC_PER_CALL);
    _filterOperator = filterOperator;
    _expressionContext = expressionContext;
    _maxSizeOfDocIdSet = maxSizeOfDocIdSet;
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public List<Operator> getChildOperators() {
    return Arrays.asList(_filterOperator);
  }

  @Nullable
  @Override
  public String toExplainString() {
    return null;
  }

  @Override
  protected DocIdSetBlock getNextBlock() {
    if (_currentDocId == Constants.EOF) {
      return null;
    }

    // Initialize filter block document Id set
    if (_filterBlockDocIdSet == null) {
      CombinedFilterBlock combinedFilterBlock = (CombinedFilterBlock) _filterOperator.nextBlock();
      FilterBlock filterBlock = combinedFilterBlock.getFilterBlock(_expressionContext);

      if (filterBlock == null) {
        throw new IllegalStateException("Null block seen");
      }

      _filterBlockDocIdSet = filterBlock.getBlockDocIdSet();
      _blockDocIdIterator = _filterBlockDocIdSet.iterator();
    }

    int pos = 0;
    int[] docIds = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];

    for (int i = 0; i < _maxSizeOfDocIdSet; i++) {
      _currentDocId = _blockDocIdIterator.next();
      if (_currentDocId == Constants.EOF) {
        break;
      }
      docIds[pos++] = _currentDocId;
    }
    if (pos > 0) {
      return new DocIdSetBlock(docIds, pos);
    } else {
      return null;
    }
  }
}
