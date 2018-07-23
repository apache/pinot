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
   * @param met 'String' metric
   * @param aggfun 'String' aggregate function
   */
  public AggfunColumnPair(String aggfun, String met) {
    _aggregatefunction = aggfun;
    _columnName = met;
  }

  /**
   * Sets the provided value into the 'aggfunc' field.
   * @param aggfunc Value to set
   */
  public void setAggregatefunction(String aggfunc) {
    _aggregatefunction = aggfunc;
  }

  /**
   * Returns the aggFunc in pair
   * @return 'String' value
   */
  public String getAggregatefunction() {
    return _aggregatefunction;
  }

  /**
   * Sets the provided value into the 'met' field.
   * @param met Value to set
   */
  public void setColumnName(String met) {
    _columnName = met;
  }

  /**
   * Returns the Metric in  pair
   * @return 'String' value
   */
  public String getColumnName() {
    return _columnName;
  }
}