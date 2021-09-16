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

import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleSet;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.query.distinct.DistinctExecutor;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.DataSchema;
import org.apache.pinot.spi.utils.DataSchema.ColumnDataType;


/**
 * Base implementation of {@link DistinctExecutor} for single raw DOUBLE column.
 */
abstract class BaseRawDoubleSingleColumnDistinctExecutor implements DistinctExecutor {
  final ExpressionContext _expression;
  final DataType _dataType;
  final int _limit;

  final DoubleSet _valueSet;

  BaseRawDoubleSingleColumnDistinctExecutor(ExpressionContext expression, DataType dataType, int limit) {
    _expression = expression;
    _dataType = dataType;
    _limit = limit;

    _valueSet = new DoubleOpenHashSet(Math.min(limit, MAX_INITIAL_CAPACITY));
  }

  @Override
  public DistinctTable getResult() {
    DataSchema dataSchema = new DataSchema(new String[]{_expression.toString()},
        new ColumnDataType[]{ColumnDataType.fromDataTypeSV(_dataType)});
    List<Record> records = new ArrayList<>(_valueSet.size());
    DoubleIterator valueIterator = _valueSet.iterator();
    while (valueIterator.hasNext()) {
      records.add(new Record(new Object[]{valueIterator.nextDouble()}));
    }
    return new DistinctTable(dataSchema, records);
  }
}
