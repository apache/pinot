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

import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * The <code>DataSchemaSegmentPruner</code> class prunes segment based on whether the all the querying columns exist in
 * the segment schema.
 */
public class DataSchemaSegmentPruner implements SegmentPruner {

  @Override
  public void init(PinotConfiguration config) {
  }

  @Override
  public boolean prune(IndexSegment segment, QueryContext query) {
    return !segment.getColumnNames().containsAll(query.getColumns());
  }

  @Override
  public String toString() {
    return "DataSchemaSegmentPruner";
  }
}
