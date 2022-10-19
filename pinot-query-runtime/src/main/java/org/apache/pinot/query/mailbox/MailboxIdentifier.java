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
   * get the sender host.
   * @return sender host
   */
  String getFromHost();

  /**
   * get the sender port.
   * @return sender port
   */
  int getFromPort();

  /**
   * get the receiver host.
   * @return receiver host
   */
  String getToHost();

  /**
   * get the receiver port.
   * @return receiver port
   */
  int getToPort();

  /**
   * Checks whether sender and receiver are in the same JVM.
   *
   * @return true if sender and receiver are in the same JVM.
   */
  boolean isLocal();
}
