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
package org.apache.pinot.common.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.pinot.common.utils.EqualityUtils;


/**
 * Class representing configurations related to realtime segment completion.
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CompletionConfig {

  @ConfigKey(value = "completionMode")
  private String _completionMode;

  public String getCompletionMode() {
    return _completionMode;
  }

  public void setCompletionMode(String completionMode) {
    _completionMode = completionMode;
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    CompletionConfig that = (CompletionConfig) o;

    return EqualityUtils.isEqual(_completionMode, that._completionMode);
  }

  @Override
  public int hashCode() {
    return EqualityUtils.hashCodeOf(_completionMode);
  }
}
