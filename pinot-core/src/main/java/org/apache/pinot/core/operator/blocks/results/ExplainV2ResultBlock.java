package org.apache.pinot.core.operator.blocks.results;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.plan.PinotExplainedRelNode;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.utils.JsonUtils;


public class ExplainV2ResultBlock extends BaseResultsBlock {

  private final QueryContext _queryContext;
  private final List<PinotExplainedRelNode.Info> _physicalPlan;

  public static final DataSchema EXPLAIN_RESULT_SCHEMA =
      new DataSchema(new String[]{"Plan"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING});

  public ExplainV2ResultBlock(QueryContext queryContext, PinotExplainedRelNode.Info physicalPlan) {
    this(queryContext, Lists.newArrayList(physicalPlan));
  }

  public ExplainV2ResultBlock(QueryContext queryContext, List<PinotExplainedRelNode.Info> physicalPlan) {
    _queryContext = queryContext;
    _physicalPlan = physicalPlan;
  }

  @Override
  public int getNumRows() {
    return _physicalPlan.size();
  }

  @Nullable
  @Override
  public QueryContext getQueryContext() {
    return _queryContext;
  }

  @Nullable
  @Override
  public DataSchema getDataSchema() {
    return DataSchema.EXPLAIN_RESULT_SCHEMA;
  }

  @Nullable
  @Override
  public List<Object[]> getRows() {
    List<Object[]> rows = new ArrayList<>(_physicalPlan.size());
    try {
      for (PinotExplainedRelNode.Info node : _physicalPlan) {
        rows.add(new Object[]{JsonUtils.objectToString(node)});
      }
      return rows;
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public DataTable getDataTable()
      throws IOException {
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(DataSchema.EXPLAIN_RESULT_SCHEMA);
    for (PinotExplainedRelNode.Info node : _physicalPlan) {
      dataTableBuilder.startRow();
      dataTableBuilder.setColumn(0, JsonUtils.objectToString(node));
      dataTableBuilder.finishRow();
    }
    return dataTableBuilder.build();
  }

  public List<PinotExplainedRelNode.Info> getPhysicalPlans() {
    return _physicalPlan;
  }
}
