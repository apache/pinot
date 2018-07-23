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

package com.linkedin.pinot.core.startreeV2;


/**
 * Utility class to store a primitive 'String' and 'String' pair.
 */
public class AggfunColumnPair {

  protected String _columnName;
  protected String _aggregatefunction;

  /**
   * Constructor for the class
   *
   * @param column 'String' column name
   * @param aggfun 'String' aggregate function
   */
  public AggfunColumnPair(String aggfun, String column) {
    _aggregatefunction = aggfun;
    _columnName = column;
  }

  /**
   * Sets the provided value into the 'aggfunc' field.
   *
   * @param aggfunc Value to set
   */
  public void setAggregatefunction(String aggfunc) {
    _aggregatefunction = aggfunc;
  }

  /**
   * Returns the aggFunc in pair
   *
   * @return 'String' value
   */
  public String getAggregatefunction() {
    return _aggregatefunction;
  }

  /**
   * Sets the provided value into the 'column' field.
   *
   * @param column Value to set
   */
  public void setColumnName(String column) {
    _columnName = column;
  }

  /**
   * Returns the column name in pair
   *
   * @return 'String' value
   */
  public String getColumnName() {
    return _columnName;
  }
}