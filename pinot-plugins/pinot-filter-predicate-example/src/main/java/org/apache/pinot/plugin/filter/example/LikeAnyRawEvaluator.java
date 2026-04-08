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
package org.apache.pinot.plugin.filter.example;

import java.util.regex.Pattern;
import org.apache.pinot.core.operator.filter.predicate.BaseRawValueBasedPredicateEvaluator;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Raw value based evaluator for LIKE_ANY predicate.
 * Matches string values against a combined regex pattern (OR of all LIKE patterns).
 */
public class LikeAnyRawEvaluator extends BaseRawValueBasedPredicateEvaluator {

  private final Pattern _pattern;

  public LikeAnyRawEvaluator(LikeAnyPredicate predicate, Pattern pattern) {
    super(predicate);
    _pattern = pattern;
  }

  @Override
  public DataType getDataType() {
    return DataType.STRING;
  }

  @Override
  public boolean applySV(String value) {
    return _pattern.matcher(value).find();
  }
}
