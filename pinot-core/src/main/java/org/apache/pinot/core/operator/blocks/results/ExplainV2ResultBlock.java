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
import org.apache.pinot.core.plan.ExplainInfo;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.utils.JsonUtils;


public class ExplainV2ResultBlock extends BaseResultsBlock {

  private final QueryContext _queryContext;
  private final List<ExplainInfo> _physicalPlan;

  public static final DataSchema EXPLAIN_RESULT_SCHEMA =
      new DataSchema(new String[]{"Plan"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING});

  public ExplainV2ResultBlock(QueryContext queryContext, ExplainInfo physicalPlan) {
    this(queryContext, Lists.newArrayList(physicalPlan));
  }

  public ExplainV2ResultBlock(QueryContext queryContext, List<ExplainInfo> physicalPlan) {
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
    return EXPLAIN_RESULT_SCHEMA;
  }

  @Nullable
  @Override
  public List<Object[]> getRows() {
    List<Object[]> rows = new ArrayList<>(_physicalPlan.size());
    try {
      for (ExplainInfo node : _physicalPlan) {
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
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(EXPLAIN_RESULT_SCHEMA);
    for (ExplainInfo node : _physicalPlan) {
      dataTableBuilder.startRow();
      dataTableBuilder.setColumn(0, JsonUtils.objectToString(node));
      dataTableBuilder.finishRow();
    }
    return dataTableBuilder.build();
  }

  public List<ExplainInfo> getPhysicalPlans() {
    return _physicalPlan;
  }
}
