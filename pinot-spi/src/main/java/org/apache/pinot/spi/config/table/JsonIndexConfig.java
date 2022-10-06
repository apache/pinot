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
package org.apache.pinot.spi.config.table;

import com.google.common.base.Preconditions;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


public class JsonIndexConfig extends BaseJsonConfig {
  private int _maxLevels = -1;
  private boolean _excludeArray = false;
  private boolean _disableCrossArrayUnnest = false;
  private Set<String> _includePaths;
  private Set<String> _excludePaths;
  private Set<String> _excludeFields;

  public int getMaxLevels() {
    return _maxLevels;
  }

  public void setMaxLevels(int maxLevels) {
    _maxLevels = maxLevels;
  }

  public boolean isExcludeArray() {
    return _excludeArray;
  }

  public void setExcludeArray(boolean excludeArray) {
    _excludeArray = excludeArray;
  }

  public boolean isDisableCrossArrayUnnest() {
    return _disableCrossArrayUnnest;
  }

  public void setDisableCrossArrayUnnest(boolean disableCrossArrayUnnest) {
    _disableCrossArrayUnnest = disableCrossArrayUnnest;
  }

  @Nullable
  public Set<String> getIncludePaths() {
    return _includePaths;
  }

  public void setIncludePaths(@Nullable Set<String> includePaths) {
    Preconditions.checkArgument(includePaths == null || _excludePaths == null,
        "Cannot configure both include and exclude paths");
    Preconditions.checkArgument(includePaths == null || includePaths.size() > 0, "Include paths cannot be empty");
    _includePaths = includePaths;
  }

  @Nullable
  public Set<String> getExcludePaths() {
    return _excludePaths;
  }

  public void setExcludePaths(@Nullable Set<String> excludePaths) {
    Preconditions.checkArgument(excludePaths == null || _includePaths == null,
        "Cannot configure both include and exclude paths");
    _excludePaths = excludePaths;
  }

  @Nullable
  public Set<String> getExcludeFields() {
    return _excludeFields;
  }

  public void setExcludeFields(@Nullable Set<String> excludeFields) {
    _excludeFields = excludeFields;
  }
}
