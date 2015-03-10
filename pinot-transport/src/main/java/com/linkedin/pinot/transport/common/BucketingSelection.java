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
package com.linkedin.pinot.transport.common;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.linkedin.pinot.common.response.ServerInstance;


/**
 *
 * This class provides an option to use a precalculated Selection to be used
 * for request. This is useful for testing where we wanted the request to go to
 * specific servers.
 * @author bvaradar
 *
 */
public class BucketingSelection {

  private final Map<SegmentId, ServerInstance> _bucketMap;

  public BucketingSelection(Map<SegmentId, ServerInstance> bucketMap) {
    if (null != bucketMap) {
      _bucketMap = bucketMap;
    } else {
      _bucketMap = new HashMap<SegmentId, ServerInstance>();
    }
  }

  /**
   *
   * Use the preselected server for a partition only if it is present in the passed list of candidates.
   *
   * @param p Partition for which selection has to happen.
   * @param orderedServers Collection of candidates from which a server has to be picked
   * @return the preselected server only if it is present in the passed list of candidates. Otherwise, it is null.
   */
  public ServerInstance selectServer(SegmentId p, Collection<ServerInstance> servers) {
    ServerInstance c = _bucketMap.get(p);
    if ((null == c) || !servers.contains(c)) {
      return null;
    }
    return c;
  }

  /**
   * Returns the pre-selected server for a partition (if pre-selected)
   * @param p Partition for which pre-selected server needs to be returned.
   * @return
   */
  public ServerInstance getPreSelectedServer(SegmentId p) {
    return _bucketMap.get(p);
  }
}
