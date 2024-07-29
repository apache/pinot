package org.apache.pinot.integration.tests.tpch.generator;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;


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
public class Column {
  private static final Random RANDOM = new Random();
  private final String _columnName;
  private final ColumnType _columnType;
  private List<String> _sampleValues;
  private boolean _isMultiValue;

  public Column(String columnName, ColumnType columnType) {
    _columnName = columnName;
    _columnType = columnType;
    _sampleValues = new ArrayList<>();
    generateSampleValues();
  }

  private void generateSampleValues() {
    switch (_columnType) {
      case STRING:
        for (int i = 0; i < 10; i++) {
          _sampleValues.add(UUID.randomUUID().toString());
        }
        break;
      case NUMERIC:
        for (int i = 0; i < 10; i++) {
          _sampleValues.add(String.valueOf(RANDOM.nextInt(1000)));
        }
        break;
      default:
        throw new IllegalStateException("Unsupported column type: " + _columnType);
    }
  }

  public boolean isMultiValue() {
    return _isMultiValue;
  }

  public void setMultiValue(boolean isMultiValue) {
    _isMultiValue = isMultiValue;
  }

  public void setSampleValues(List<String> sampleValues) {
    _sampleValues = sampleValues;
  }

  public String getColumnName() {
    return _columnName;
  }

  public String getColumnNameForPredicate(String prefix) {
    return prefix + _columnName;
  }

  public String getColumnNameForPinotPredicate(String prefix) {
    if (isMultiValue()) {
      return "arrayToMv(" + prefix + _columnName + ")";
    } else {
      return prefix + _columnName;
    }
  }

  public ColumnType getColumnType() {
    return _columnType;
  }

  public String getRandomStringValue() {
    return _sampleValues.get(RANDOM.nextInt(_sampleValues.size()));
  }

  public long getRandomNumericValue() {
    return (long) Double.parseDouble(getRandomStringValue());
  }
}
