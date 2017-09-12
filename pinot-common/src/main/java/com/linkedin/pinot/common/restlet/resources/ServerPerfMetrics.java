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

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

/*
Server measure defined performance metrics as of object of this class
This class can be expanded to contain more load metrics
 */

@JsonIgnoreProperties(ignoreUnknown = true)
public class ServerPerfMetrics {
  public long numOfSegments = 0;
  public long segmentsDiskSizeInBytes = 0;

  public ServerPerfMetrics() {
    numOfSegments = 0;
    segmentsDiskSizeInBytes = 0;
  }

  public long getNumOfSegments() {
    return numOfSegments;
  }

  public long getSegmentsDiskSizeInBytes() {
    return segmentsDiskSizeInBytes;
  }
}
