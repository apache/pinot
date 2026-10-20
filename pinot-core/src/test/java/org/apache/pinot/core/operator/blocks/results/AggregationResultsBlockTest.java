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

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class AggregationResultsBlockTest {

  /// The response metadata is read before the data table is built, so a server that returns final results has to
  /// extract them early to report a SMART function that switched to a sketch.
  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testServerFinalResultReportsApproximatedResult()
      throws IOException {
    for (int threshold : new int[]{1, 10}) {
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
          "SELECT DISTINCTCOUNTSMARTHLL(col, 'threshold=" + threshold + "') FROM testTable");
      queryContext.setServerReturnFinalResult(true);
      AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
      Object merged =
          aggregationFunctions[0].merge(new IntOpenHashSet(new int[]{1}), new IntOpenHashSet(new int[]{2}));
      AggregationResultsBlock resultsBlock =
          new AggregationResultsBlock(aggregationFunctions, new ArrayList<>(List.of(merged)), queryContext);

      Map<String, String> metadata = resultsBlock.getResultsMetadata();
      assertEquals(Boolean.parseBoolean(metadata.get(MetadataKey.APPROXIMATED_RESULT.getName())), threshold == 1);
      DataTable dataTable = resultsBlock.getDataTable();
      assertEquals(dataTable.getNumberOfRows(), 1);
      assertEquals(resultsBlock.getRows().get(0)[0], dataTable.getInt(0, 0));
    }
  }
}
