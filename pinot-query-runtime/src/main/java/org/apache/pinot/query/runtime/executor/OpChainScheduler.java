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
package org.apache.pinot.query.runtime.executor;

import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.runtime.operator.OpChain;


/**
 * An interface that defines different scheduling strategies to work with the
 * {@link OpChainSchedulerService}. All methods are thread safe and can be guaranteed
 * to never be called concurrently - therefore all implementations may use data
 * structures that are not concurrent.
 */
public interface OpChainScheduler {

  /**
   * @param operatorChain the operator chain to register
   * @param isNew         whether or not this is the first time the operator is scheduled
   */
  void register(OpChain operatorChain, boolean isNew);

  /**
   * This method is called whenever {@code mailbox} has new data available to consume,
   * this can be useful for advanced scheduling algorithms
   *
   * @param mailbox the mailbox ID
   */
  void onDataAvailable(MailboxIdentifier mailbox);

  /**
   * @return whether or not there is any work for the scheduler to do
   */
  boolean hasNext();

  /**
   * @return the next operator chain to process
   * @throws java.util.NoSuchElementException if {@link #hasNext()} returns false
   *         prior to this call
   */
  OpChain next();

  /**
   * @return the number of operator chains that are awaiting execution
   */
  int size();
}
