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
package com.linkedin.pinot.core.query.pruner;

import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.request.ServerQueryRequest;
import org.apache.commons.configuration.Configuration;


public interface SegmentPruner {

  /**
   * Initializes the segment pruner.
   */
  void init(Configuration config);

  /**
   * Returns <code>true</code> if the segment can be pruned based on the query request.
   */
  boolean prune(IndexSegment segment, ServerQueryRequest queryRequest);
}
