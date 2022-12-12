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

import java.util.List;
import java.util.Set;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


public abstract class V2Operator extends BaseOperator<TransferableBlock> implements ScheduledOperator {

  @Override
  public ScheduleResult shouldSchedule(Set<MailboxIdentifier> availableMail) {
    List<Operator> children = getChildOperators();

    if (children.size() != 1) {
      throw new UnsupportedOperationException(this.getClass() + " must implement shouldSchedule()");
    } else if (!(children.get(0) instanceof V2Operator)) {
      throw new IllegalStateException("V2 Operators must only rely on other V2 operators. Got: "
          + children.get(0).getClass());
    }

    return ((V2Operator) children.get(0)).shouldSchedule(availableMail);
  }
}
