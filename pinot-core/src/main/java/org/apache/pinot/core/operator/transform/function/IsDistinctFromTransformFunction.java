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
package org.apache.pinot.core.operator.transform.function;

/**
 * The <code>IsDistinctFromTransformFunction</code> extends <code>DistinctFromTransformFunction</code> to implement the
 * IS_DISTINCT_FROM operator.
 *
 * The results are in boolean format and stored as an integer array with 1 represents true and 0 represents false.
 * Expected result:
 * NUll IS_DISTINCT_FROM Value: 1
 * NUll IS_DISTINCT_FROM Null: 0
 * ValueA IS_DISTINCT_FROM ValueB: NotEQUALS(ValueA, ValueB)
 *
 * Note this operator only takes column names for now.
 * SQL Syntax:
 *    columnA IS DISTINCT FROM columnB
 *
 * Sample Usage:
 *    IS_DISTINCT_FROM(columnA, columnB)
 */
public class IsDistinctFromTransformFunction extends DistinctFromTransformFunction {
  public IsDistinctFromTransformFunction() {
    super(true);
  }
}
