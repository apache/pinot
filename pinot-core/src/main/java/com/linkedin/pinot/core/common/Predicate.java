/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.common;

import java.util.Arrays;
import java.util.List;


public class Predicate {

  public enum Type {
    EQ,
    NEQ,
    REGEX,
    RANGE,
    IN,
    NOT_IN
  };

  //	public Predicate(String lhs, Type predicateType, int rhs){
  //		this.lhs = lhs;
  //		type = predicateType;
  //		this.rhs = rhs;
  //	}

  public Predicate(String lhs, Type predicateType, List<String> rhs) {
    this.lhs = lhs;
    type = predicateType;
    this.rhs = rhs;
  }
  String lhs;

  List<String> rhs;

  Type type;

  public String getLhs() {
    return lhs;
  }

  public List<String> getRhs() {
    return rhs;
  }

  public Type getType() {
    return type;
  }

  @Override
  public String toString() {
    return "Predicate: type: " + type + ", left : " + lhs + ", right : " + Arrays.toString(rhs.toArray(new String[0]))
        + "\n";
  }

}
