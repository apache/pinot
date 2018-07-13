/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.common.predicate;

import com.linkedin.pinot.core.common.Predicate;
import java.util.Arrays;
import java.util.List;


/**
 * Abstract base class for IN and NOT IN predicates.
 */
public abstract class BaseInPredicate extends Predicate {
  public static final String DELIMITER = "\t\t";

  public BaseInPredicate(String lhs, Type predicateType, List<String> rhs) {
    super(lhs, predicateType, rhs);
  }

  @Override
  public String toString() {
    List<String> rhs = getRhs();
    return "Predicate: type: " + getType() + ", left : " + getLhs() + ", right : " + Arrays.toString(
        rhs.toArray(new String[rhs.size()])) + "\n";
  }

  public String[] getValues() {
    /* To maintain backward compatibility, we always split if number of values is one. We do not support
       case where DELIMITER is a sub-string of value.
       TODO: Clean this up after broker changes to enable splitting have been around for sometime.
     */
    List<String> values = getRhs();
    return (values.size() > 1) ? values.toArray(new String[values.size()]) : values.get(0).split(DELIMITER);
  }
}
