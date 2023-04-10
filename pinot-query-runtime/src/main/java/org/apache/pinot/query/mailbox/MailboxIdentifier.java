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
package org.apache.pinot.query.mailbox;

import org.apache.pinot.query.routing.VirtualServerAddress;


/**
 * {@link MailboxIdentifier} uniquely identify the mailbox that pairs a sender and a receiver.
 *
 * <p>It consists of the job_id and the partition key. as well as the component for a channelID.
 */
public interface MailboxIdentifier {

  /**
   * get the job identifier.
   * @return job identifier.
   */
  String getJobId();

  /**
   * @return the sender address
   */
  VirtualServerAddress getFromHost();

  /**
   * @return the destination address
   */
  VirtualServerAddress getToHost();

  /**
   * Checks whether sender and receiver are in the same JVM.
   *
   * @return true if sender and receiver are in the same JVM.
   */
  boolean isLocal();

  /**
   * @return stage-id of the stage that is sending the data across this mailbox.
   */
  int getSenderStageId();

  /**
   * @return stage-id of the stage that is receiving the data from this mailbox.
   */
  int getReceiverStageId();
}
