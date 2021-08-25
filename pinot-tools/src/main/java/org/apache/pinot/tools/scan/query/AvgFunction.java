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


public class AvgFunction extends AggregationFunc {
  private static final String NAME = "avg";

  AvgFunction(ResultTable rows, String column) {
    super(rows, column);
  }

  @Override
  public ResultTable run() {
    Double sum = 0.0;
    int numEntries = 0;

    for (ResultTable.Row row : _rows) {
      Object value = row.get(_column, NAME);
      if (value instanceof double[]) {
        double[] valArray = (double[]) value;
        sum += valArray[0];
        numEntries += valArray[1];
      } else {
        sum += new Double(row.get(_column, NAME).toString());
        ++numEntries;
      }
    }

    double[] average = new double[2];
    average[0] = sum;
    average[1] = numEntries;

    ResultTable resultTable = new ResultTable(new ArrayList<Pair>(), 1);
    resultTable.add(0, average);
    return resultTable;
  }
}
