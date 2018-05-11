/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.restlet.resources;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;

/**
Server measure defined performance metrics as of object of this class
This class can be expanded to contain more load metrics
 */

@JsonIgnoreProperties(ignoreUnknown = true)
public class ServerPerfMetrics {
  public long segmentCount = 0;
  public long segmentsHitCount = 0;
  public long segmentDiskSizeInBytes = 0;
  public double segmentCPULoad = 0;
  public List<String> tableList;
  public List<List<Long>> segmentTimeInfo;

  //public List<SegmentMetadata> segmentList;

  public ServerPerfMetrics() {
    segmentCount = 0;
    segmentsHitCount = 0;
    segmentDiskSizeInBytes = 0;
    segmentCPULoad = 0;

    tableList = new ArrayList <>();
    segmentTimeInfo = new ArrayList <>();

  }

  public long getSegmentCount() {
    return segmentCount;
  }
  public long getSegmentsHitCount() { return segmentsHitCount; }
  public long getSegmentDiskSizeInBytes() {
    return segmentDiskSizeInBytes;
  }
}
