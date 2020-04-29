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
package org.apache.pinot.core.segment.rollup;

import java.util.List;
import org.apache.commons.configuration.Configuration;


public class MergeRollupMetadata {
  private final List<String> _groupsFrom;
  private final List<String> _segmentsTo;

  public MergeRollupMetadata(String groupId, List<String> groupsFrom, List<String> segmentsTo) {
    _groupsFrom = groupsFrom;
    _segmentsTo = segmentsTo;
  }

  public MergeRollupMetadata(Configuration metadataProperties) {
    _groupsFrom = metadataProperties.getList(MergeRollupConstants.MetadataKey.GROUPS_FROM);
    _segmentsTo = metadataProperties.getList(MergeRollupConstants.MetadataKey.SEGMENTS_TO);
  }

  public List<String> getGroupsFrom() {
    return _groupsFrom;
  }

  public List<String> getSegmentsTo() {
    return _segmentsTo;
  }
}
