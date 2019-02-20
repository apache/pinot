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
package org.apache.pinot.core.query.scheduler.fcfs;

import javax.annotation.Nonnull;
import org.apache.pinot.core.query.scheduler.AbstractSchedulerGroup;
import org.apache.pinot.core.query.scheduler.SchedulerGroup;
import org.apache.pinot.core.query.scheduler.SchedulerGroupAccountant;
import org.apache.pinot.core.query.scheduler.SchedulerQueryContext;


public class FCFSSchedulerGroup extends AbstractSchedulerGroup {

  public FCFSSchedulerGroup(@Nonnull String group) {
    super(group);
  }

  /**
   * Group that has pending query with earlier arrival time has higher priority.
   * @param rhs
   * @return 1 if this has lower arrival time than rhs
   *         -1 if this has higher arrival time than lhs
   *         0 if arrival times are equal
   */
  @Override
  public int compareTo(SchedulerGroupAccountant rhs) {
    return compare(this, ((SchedulerGroup) rhs));
  }

  public static int compare(SchedulerGroup lhs, SchedulerGroup rhs) {
    if (lhs == rhs) {
      return 0;
    }
    if (rhs == null) {
      return 1;
    }

    SchedulerQueryContext lhsFirst = lhs.peekFirst();
    SchedulerQueryContext rhsFirst = rhs.peekFirst();
    if (lhsFirst != null && rhsFirst != null) {
      return Long.compare(rhsFirst.getArrivalTimeMs(), lhsFirst.getArrivalTimeMs());
    } else if (lhsFirst != null) {
      return 1;
    } else if (rhsFirst != null) {
      return -1;
    } else {
      return 0;
    }
  }
}
