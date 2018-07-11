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

import java.util.Arrays;
import java.util.List;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.core.common.Predicate;


public class RegexpLikePredicate extends Predicate {
  String _regex;
  public RegexpLikePredicate(String lhs, List<String> rhs) {
    super(lhs, Type.REGEXP_LIKE, rhs);
    //we should have atleast one regex, we will ignore the remaining part instead of throwing exception
    Preconditions.checkArgument(rhs.size() >= 1);
    _regex = rhs.get(0);
  }

  @Override
  public String toString() {
    return "Predicate: type: " + getType() + ", left : " + getLhs() + ", right : " + Arrays.toString(getRhs().toArray(new String[0])) + "\n";
  }
  
  public String getRegex(){
   return _regex; 
  }

}
