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
package org.apache.pinot.common.request.context.predicate;

import org.apache.pinot.common.request.context.ExpressionContext;

/**
 * Predicate which represents a LIKE operator
 */
public class LikePredicate extends RegexpLikePredicate {
  public static final String[] REGEXP_METACHARACTERS  = {"\\","^","$","{","}","[","]","(",")",".",
      "*","+","?","|","<",">","-","&"};

  public LikePredicate(ExpressionContext lhs, String value) {
    super(lhs, processValue(value));
  }

  @Override
  public Type getType() {
    return Type.LIKE;
  }

  @Override
  public String toString() {
    return _lhs + "LIKE" + _value;
  }

  private static String processValue(String value) {
    //String result = Pattern.quote(value);

    String result = escapeMetaCharacters(value);
    result = result.replace(".", "\\.");
    // ... escape any other potentially problematic characters here
    result = result.replace("?", ".");

    return result.replaceAll("(?<!\\\\)%", ".*");
  }

  /**
   * Add escape characters before special characters
   * @param inputString
   * @return
   */
  private static String escapeMetaCharacters(String inputString){

    for (int i = 0 ; i < REGEXP_METACHARACTERS.length ; i++){
      if(inputString.contains(REGEXP_METACHARACTERS[i])){
        inputString = inputString.replace(REGEXP_METACHARACTERS[i],"\\"
            + REGEXP_METACHARACTERS[i]);
      }
    }
    return inputString;
  }
}
