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
package org.apache.pinot.core.query.distinct.raw;

import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.query.distinct.DistinctExecutor;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.core.query.request.context.ExpressionContext;


/**
 * Base implementation of {@link DistinctExecutor} for single raw INT column.
 */
abstract class BaseRawIntSingleColumnDistinctExecutor implements DistinctExecutor {
  final ExpressionContext _expression;
  final int _limit;

  final IntSet _valueSet;

  BaseRawIntSingleColumnDistinctExecutor(ExpressionContext expression, int limit) {
    _expression = expression;
    _limit = limit;

    _valueSet = new IntOpenHashSet(Math.min(limit, MAX_INITIAL_CAPACITY));
  }

  @Override
  public DistinctTable getResult() {
    DataSchema dataSchema =
        new DataSchema(new String[]{_expression.toString()}, new ColumnDataType[]{ColumnDataType.DOUBLE});
    List<Record> records = new ArrayList<>(_valueSet.size());
    IntIterator valueIterator = _valueSet.iterator();
    while (valueIterator.hasNext()) {
      records.add(new Record(new Object[]{valueIterator.nextInt()}));
    }
    return new DistinctTable(dataSchema, records);
  }
}
