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
package org.apache.pinot.spi.systemtable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;


/**
 * Request sent to a {@link SystemTableProvider} from the system query engine.
 */
public final class SystemTableRequest {
  private final List<String> _projections;
  private final @Nullable SystemTableFilter _filter;
  private final int _offset;
  private final int _limit;

  public SystemTableRequest(List<String> projections, @Nullable SystemTableFilter filter, int offset, int limit) {
    _projections = Collections.unmodifiableList(
        new ArrayList<>(Objects.requireNonNull(projections, "projections must not be null")));
    _filter = filter;
    _offset = Math.max(0, offset);
    _limit = limit;
  }

  /**
   * Column names requested in the projection. Empty list means all columns.
   */
  public List<String> getProjections() {
    return _projections;
  }

  public @Nullable SystemTableFilter getFilter() {
    return _filter;
  }

  public int getOffset() {
    return _offset;
  }

  public int getLimit() {
    return _limit;
  }
}
