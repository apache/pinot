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

package org.apache.pinot.query.runtime.operator;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.pinot.query.mailbox.MailboxIdentifier;


/**
 * {@code ScheduledOperator}s are operators that expose additional information
 * to help scheduling logic.
 */
public interface ScheduledOperator {

  /**
   * @param availableMail the mail available to be consumed
   * @return the mailboxes that this operator would read from if scheduled - an
   * empty set indicates that this operator should not be scheduled
   */
  ScheduleResult shouldSchedule(Set<MailboxIdentifier> availableMail);

  class ScheduleResult {
    public final boolean _shouldSchedule;
    public final Set<MailboxIdentifier> _mailboxes;

    public ScheduleResult(Set<MailboxIdentifier> mailboxes) {
      _shouldSchedule = !mailboxes.isEmpty();
      _mailboxes = mailboxes;
    }

    public ScheduleResult(boolean shouldSchedule) {
      _shouldSchedule = shouldSchedule;
      _mailboxes = ImmutableSet.of();
    }
  }
}
