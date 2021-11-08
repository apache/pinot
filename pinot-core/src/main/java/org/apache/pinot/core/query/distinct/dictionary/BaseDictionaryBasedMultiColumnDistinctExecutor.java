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
package org.apache.pinot.core.query.distinct.dictionary;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.query.distinct.DistinctExecutor;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.DataSchema;
import org.apache.pinot.spi.data.DataSchema.ColumnDataType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Base implementation of {@link DistinctExecutor} for multiple dictionary-encoded columns.
 */
abstract class BaseDictionaryBasedMultiColumnDistinctExecutor implements DistinctExecutor {
  final List<ExpressionContext> _expressions;
  final List<Dictionary> _dictionaries;
  final List<DataType> _dataTypes;
  final int _limit;

  final ObjectSet<DictIds> _dictIdsSet;

  BaseDictionaryBasedMultiColumnDistinctExecutor(List<ExpressionContext> expressions, List<Dictionary> dictionaries,
      List<DataType> dataTypes, int limit) {
    _expressions = expressions;
    _dictionaries = dictionaries;
    _dataTypes = dataTypes;
    _limit = limit;

    _dictIdsSet = new ObjectOpenHashSet<>(Math.min(limit, MAX_INITIAL_CAPACITY));
  }

  @Override
  public DistinctTable getResult() {
    int numExpressions = _expressions.size();
    String[] columnNames = new String[numExpressions];
    ColumnDataType[] columnDataTypes = new ColumnDataType[numExpressions];
    for (int i = 0; i < numExpressions; i++) {
      columnNames[i] = _expressions.get(i).toString();
      columnDataTypes[i] = ColumnDataType.fromDataTypeSV(_dataTypes.get(i));
    }
    DataSchema dataSchema = new DataSchema(columnNames, columnDataTypes);
    List<Record> records = new ArrayList<>(_dictIdsSet.size());
    for (DictIds dictIds : _dictIdsSet) {
      Object[] values = new Object[numExpressions];
      for (int i = 0; i < numExpressions; i++) {
        int dictId = dictIds._dictIds[i];
        values[i] = _dictionaries.get(i).getInternal(dictId);
      }
      records.add(new Record(values));
    }
    return new DistinctTable(dataSchema, records);
  }

  static class DictIds {
    final int[] _dictIds;

    DictIds(int[] dictIds) {
      _dictIds = dictIds;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object o) {
      return Arrays.equals(_dictIds, ((DictIds) o)._dictIds);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(_dictIds);
    }
  }
}
