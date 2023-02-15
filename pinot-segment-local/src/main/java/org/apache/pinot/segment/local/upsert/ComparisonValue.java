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
package org.apache.pinot.segment.local.upsert;

import javax.annotation.Nonnull;


@SuppressWarnings({"rawtypes", "unchecked"})
public class ComparisonValue implements Comparable {
  private final Comparable _comparisonValue;
  private final boolean _isNull;

  public ComparisonValue(Comparable comparisonValue) {
    this(comparisonValue, false);
  }

  public ComparisonValue(Comparable comparisonValue, boolean isNull) {
    _comparisonValue = comparisonValue;
    _isNull = isNull;
  }

  public Comparable getComparisonValue() {
    return _comparisonValue;
  }

  public boolean isNull() {
    return _isNull;
  }

  @Override
  public int compareTo(@Nonnull Object o) {
    if (o instanceof ComparisonValue) {
      return _comparisonValue.compareTo(((ComparisonValue) o).getComparisonValue());
    }
    return _comparisonValue.compareTo(o);
  }
}
