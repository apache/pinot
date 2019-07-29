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
package org.apache.pinot.tools.tuner.meta.manager.collector;

import java.io.File;
import javax.annotation.Nonnull;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.AbstractQueryStats;


public class PathWrapper extends AbstractQueryStats {
  private File _file;

  @Override
  public String toString() {
    return "PathWrapper{" + "_path='" + _file + '\'' + ", _tableNameWithoutType='" + _tableNameWithoutType + '\'' + '}';
  }

  public File getFile() {
    return _file;
  }

  public String getTableNameWithoutType() {
    return _tableNameWithoutType;
  }

  private PathWrapper(Builder builder) {
    _tableNameWithoutType = builder._tableNameWithoutType;
    _file = builder._file;
  }

  public static final class Builder {
    private String _tableNameWithoutType;
    private File _file;

    public Builder() {
    }

    @Nonnull
    public Builder setTableNameWithoutType(@Nonnull String val) {
      _tableNameWithoutType = val;
      return this;
    }

    @Nonnull
    public Builder setFile(@Nonnull File val) {
      _file = val;
      return this;
    }

    @Nonnull
    public PathWrapper build() {
      return new PathWrapper(this);
    }
  }
}
