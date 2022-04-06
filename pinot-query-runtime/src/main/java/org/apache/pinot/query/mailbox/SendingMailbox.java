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
 * Mailbox is used to send and receive data.
 *
 * Mailbox should be instantiated on both side of MailboxServer.
 *
 * @param <T> type of data carried over the mailbox.
 */
public interface SendingMailbox<T> {

  /**
   * get the unique identifier for the mailbox.
   *
   * @return Mailbox ID.
   */
  String getMailboxId();

  /**
   * send a data packet through the mailbox.
   * @param data
   * @throws UnsupportedOperationException
   */
  void send(T data)
      throws UnsupportedOperationException;

  /**
   * Complete delivery of the current mailbox.
   */
  void complete();
}
