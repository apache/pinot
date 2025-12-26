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
package org.apache.pinot.common.systemtable;

import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.systemtable.SystemTableProvider;


/**
 * Extension of {@link SystemTableProvider} that can supply data for a system table query using the standard v1 query
 * engine.
 */
public interface SystemTableDataProvider extends SystemTableProvider {

  /**
   * Returns an {@link IndexSegment} representing the system table contents.
   * <p>
   * The returned segment is used for query execution on the broker. Implementations may return a new segment for each
   * call (recommended) or a cached segment. Callers should invoke {@link IndexSegment#destroy()} when done with the
   * returned segment.
   */
  IndexSegment getDataSource()
      throws Exception;
}
