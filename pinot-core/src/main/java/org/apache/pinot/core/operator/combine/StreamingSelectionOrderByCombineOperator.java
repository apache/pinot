package org.apache.pinot.core.operator.combine;

import java.util.List;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.combine.merger.ResultsBlockMerger;
import org.apache.pinot.core.query.request.context.QueryContext;


public class StreamingSelectionOrderByCombineOperator<T extends BaseResultsBlock> extends BaseCombineOperator<T> {
  public StreamingSelectionOrderByCombineOperator(ResultsBlockMerger<T> resultsBlockMerger, List<Operator> operators,
      QueryContext queryContext, ExecutorService executorService) {
    super(resultsBlockMerger, operators, queryContext, executorService);
  }

  @Override
  protected void processSegments() {
  }

  @Override
  protected void onProcessSegmentsException(Throwable t) {

  }

  @Override
  protected void onProcessSegmentsFinish() {

  }

  @Override
  protected BaseResultsBlock getNextBlock() {
    return null;
  }

  @Nullable
  @Override
  public String toExplainString() {
    return "";
  }
}
