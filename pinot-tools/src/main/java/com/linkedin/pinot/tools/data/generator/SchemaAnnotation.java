/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.tools.data.generator;

public class SchemaAnnotation {
  String _column;
  boolean _range;

  private int _cardinality;
  private int _rangeStart;
  private int _rangeEnd;

  public SchemaAnnotation() {
  }

  public SchemaAnnotation(String column, int cardinality) {
    _column = column;
    _cardinality = cardinality;
  }

  public SchemaAnnotation(String column, int rangeStart, int rangeEnd) {
    _column = column;
    _rangeStart = rangeStart;
    _rangeEnd = rangeEnd;
  }

  public String getColumn() {
    return _column;
  }

  public void setColumn(String column) {
    _column = column;
  }

  public boolean isRange() {
    return _range;
  }

  public void setRange(boolean range) {
    _range = range;
  }

  public int getCardinality() {
    return _cardinality;
  }

  public void setCardinality(int cardinality) {
    _cardinality = cardinality;
  }

  public int getRangeStart() {
    return _rangeStart;
  }

  public void setRangeStart(int rangeStart) {
    _rangeStart = rangeStart;
  }

  public int getRangeEnd() {
    return _rangeEnd;
  }

  public void setRangeEnd(int rangeEnd) {
    _rangeEnd = rangeEnd;
  }
}
