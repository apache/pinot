/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.data.manager;

import com.linkedin.pinot.core.indexsegment.IndexSegment;


/**
 * An immutable wrapper of IndexSegment.
 *
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public class SegmentDataManager {

  private final IndexSegment _indexSegment;

  public SegmentDataManager(IndexSegment indexSegment) {
    _indexSegment = indexSegment;
  }

  public IndexSegment getSegment() {
    return _indexSegment;
  }

  public String getSegmentName() {
    return _indexSegment.getSegmentName();
  }

  public String toString() {
    return "SegmentDataManager { " + _indexSegment.getSegmentName() + " } ";
  }
}
