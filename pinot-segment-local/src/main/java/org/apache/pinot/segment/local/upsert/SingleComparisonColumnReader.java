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
package org.apache.pinot.segment.local.upsert;

import java.io.IOException;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.spi.IndexSegment;


@SuppressWarnings("rawtypes")
public class SingleComparisonColumnReader implements UpsertUtils.ComparisonColumnReader {
  private final PinotSegmentColumnReader _comparisonColumnReader;

  public SingleComparisonColumnReader(IndexSegment segment, String comparisonColumn) {
    _comparisonColumnReader = new PinotSegmentColumnReader(segment, comparisonColumn);
  }

  @Override
  public Comparable getComparisonValue(int docId) {
    return (Comparable) _comparisonColumnReader.getValue(docId);
  }

  @Override
  public void close()
      throws IOException {
    _comparisonColumnReader.close();
  }
}
