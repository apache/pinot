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
package org.apache.pinot.segment.local.segment.virtualcolumn;


/// Virtual column provider for `$totalDocs`, the number of documents in the segment.
///
/// The count is taken from the virtual column context rather than from `SegmentMetadata`,
/// so that a CONSUMING segment reports the number of documents indexed so far instead of `0`.
///
/// This is the number of documents physically stored in the segment, so for an upsert table it also includes the
/// documents that have been replaced and are no longer returned by queries.
public class SegmentTotalDocsVirtualColumnProvider extends BaseConstantValueVirtualColumnProvider {

  @Override
  protected Object getValue(VirtualColumnContext context) {
    return context.getTotalDocCount();
  }
}
