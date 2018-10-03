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
import java.util.Set;
import org.apache.commons.configuration.Configuration;


/**
 * The <code>DataSchemaSegmentPruner</code> class prunes segment based on whether the all the querying columns exist in
 * the segment schema.
 */
public class DataSchemaSegmentPruner implements SegmentPruner {

  @Override
  public void init(Configuration config) {
  }

  @Override
  public boolean prune(IndexSegment segment, ServerQueryRequest queryRequest) {
    Set<String> columnsInSchema = segment.getSegmentMetadata().getSchema().getColumnNames();
    return !columnsInSchema.containsAll(queryRequest.getAllColumns());
  }

  @Override
  public String toString() {
    return "DataSchemaSegmentPruner";
  }
}
