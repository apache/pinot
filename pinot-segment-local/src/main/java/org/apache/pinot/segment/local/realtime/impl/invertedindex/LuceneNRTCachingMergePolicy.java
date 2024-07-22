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
package org.apache.pinot.segment.local.realtime.impl.invertedindex;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.NRTCachingDirectory;


/**
 * LuceneNRTCachingMergePolicy is a best-effort policy to generate merges for segments that are fully in memory,
 * at the time of SegmentInfo selection. It does not consider segments that have been flushed to disk eligible
 * for merging.
 * <p>
 * Each refresh creates a small Lucene segment. Increasing the frequency of refreshes to reduce indexing lag results
 * in a large number of small segments, and high disk IO ops for merging them. By using this best-effort merge policy
 * the small ops can be avoided since the segments are in memory when merged.
 */
public class LuceneNRTCachingMergePolicy extends TieredMergePolicy {
  private final NRTCachingDirectory _nrtCachingDirectory;

  public LuceneNRTCachingMergePolicy(NRTCachingDirectory nrtCachingDirectory) {
    _nrtCachingDirectory = nrtCachingDirectory;
  }

  @Override
  public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
      throws IOException {
    SegmentInfos inMemorySegmentInfos = new SegmentInfos(segmentInfos.getIndexCreatedVersionMajor());
    // Collect all segment commit infos that still have all files in memory
    Set<String> cachedFiles = new HashSet<>(List.of(_nrtCachingDirectory.listCachedFiles()));
    for (SegmentCommitInfo info : segmentInfos) {
      for (String file : info.files()) {
        if (!cachedFiles.contains(file)) {
          break;
        }
      }
      inMemorySegmentInfos.add(info);
    }
    return super.findMerges(mergeTrigger, inMemorySegmentInfos, mergeContext);
  }
}
