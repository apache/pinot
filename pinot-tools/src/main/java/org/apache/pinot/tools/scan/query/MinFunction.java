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


public class MinFunction extends AggregationFunc {
  private static final String _name = "min";

  MinFunction(ResultTable rows, String columnId) {
    super(rows, columnId);
  }

  @Override
  public ResultTable run() {
    Double min = Double.MAX_VALUE;

    for (ResultTable.Row row : _rows) {
      min = Math.min(min, new Double(row.get(_column, _name).toString()));
    }

    ResultTable resultTable = new ResultTable(new ArrayList<Pair>(), 1);
    resultTable.add(0, min);

    return resultTable;
  }
}
