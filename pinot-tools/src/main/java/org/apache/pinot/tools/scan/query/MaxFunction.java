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
package org.apache.pinot.tools.scan.query;

import java.util.ArrayList;
import org.apache.pinot.spi.utils.Pair;


public class MaxFunction extends AggregationFunc {
  private static final String NAME = "max";

  MaxFunction(ResultTable rows, String column) {
    super(rows, column);
  }

  @Override
  public ResultTable run() {
    Double max = Double.NEGATIVE_INFINITY;

    for (ResultTable.Row row : _rows) {
      max = Math.max(max, new Double(row.get(_column, NAME).toString()));
    }

    ResultTable resultTable = new ResultTable(new ArrayList<Pair>(), 1);
    resultTable.add(0, max);

    return resultTable;
  }
}
