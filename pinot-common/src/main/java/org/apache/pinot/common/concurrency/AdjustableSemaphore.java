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
package org.apache.pinot.common.concurrency;

import java.util.concurrent.Semaphore;


/**
 * A semaphore that allows adjusting the number of permits in a non-blocking way.
 */
public class AdjustableSemaphore extends Semaphore {

  private int _totalPermits;

  public AdjustableSemaphore(int permits) {
    super(permits);
    _totalPermits = permits;
  }

  public AdjustableSemaphore(int permits, boolean fair) {
    super(permits, fair);
    _totalPermits = permits;
  }

  public void setPermits(int permits) {
    if (permits < _totalPermits) {
      reducePermits(_totalPermits - permits);
    } else if (permits > _totalPermits) {
      release(permits - _totalPermits);
    }
    _totalPermits = permits;
  }
}
