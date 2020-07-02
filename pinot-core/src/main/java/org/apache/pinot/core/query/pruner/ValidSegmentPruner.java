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
package org.apache.pinot.core.query.pruner;

import org.apache.commons.configuration.Configuration;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadata;


/**
 * Segment pruner class that implements checks to prune a segment with
 * invalid/bad data.
 */
public class ValidSegmentPruner implements SegmentPruner {
  @Override
  public void init(Configuration config) {

  }

  /**
   * Returns true if a segment should be pruned-out due to bad/invalid data.
   * Current check(s) below:
   * - Empty segment.
   *
   * @param segment
   * @param queryRequest
   * @return
   */
  @Override
  public boolean prune(IndexSegment segment, ServerQueryRequest queryRequest) {
    SegmentMetadata segmentMetadata = segment.getSegmentMetadata();

    // Check for empty segment.
    if (segmentMetadata.getTotalDocs() == 0) {
      return true;
    }

    // Add more validation here.

    return false;
  }

  @Override
  public String toString() {
    return "ValidSegmentPruner";
  }
}
