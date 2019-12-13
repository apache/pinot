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
package org.apache.pinot.core.common.predicate;

import org.apache.commons.lang3.StringUtils;


public class RangePredicate implements Predicate {
  public static final String DELIMITER = "\t\t";
  public static final char LOWER_INCLUSIVE = '[';
  public static final char LOWER_EXCLUSIVE = '(';
  public static final char UPPER_INCLUSIVE = ']';
  public static final char UPPER_EXCLUSIVE = ')';
  public static final String UNBOUNDED = "*";

  private final boolean _includeLowerBoundary;
  private final boolean _includeUpperBoundary;
  private final String _lowerBoundary;
  private final String _upperBoundary;

  public RangePredicate(String value) {
    _includeLowerBoundary = value.charAt(0) == LOWER_INCLUSIVE;
    _includeUpperBoundary = value.charAt(value.length() - 1) == UPPER_INCLUSIVE;

    String[] parts = StringUtils.splitByWholeSeparator(value, DELIMITER);
    _lowerBoundary = parts[0].substring(1);
    _upperBoundary = parts[1].substring(0, parts[1].length() - 1);
  }

  public boolean includeLowerBoundary() {
    return _includeLowerBoundary;
  }

  public boolean includeUpperBoundary() {
    return _includeUpperBoundary;
  }

  public String getLowerBoundary() {
    return _lowerBoundary;
  }

  public String getUpperBoundary() {
    return _upperBoundary;
  }

  @Override
  public Type getType() {
    return Type.RANGE;
  }
}
