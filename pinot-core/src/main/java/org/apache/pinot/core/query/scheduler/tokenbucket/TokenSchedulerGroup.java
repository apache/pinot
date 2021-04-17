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
package org.apache.pinot.core.query.scheduler.tokenbucket;

import com.google.common.base.Preconditions;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.pinot.core.query.scheduler.AbstractSchedulerGroup;
import org.apache.pinot.core.query.scheduler.SchedulerGroup;
import org.apache.pinot.core.query.scheduler.SchedulerGroupAccountant;
import org.apache.pinot.core.query.scheduler.fcfs.FCFSSchedulerGroup;


/**
 * Scheduler group that manages accounting based on the number of tokens.
 *
 * Each SchedulerGroup is allotted a set of token periodically. Token represents
 * a unit of thread wall clock time. Tokens are deducted from group for each unit
 * of time per thread that this group uses. New batch of tokens are allotted periodically
 * by applying linear decay. Linear decay memorizes resource utilization in the previous
 * time quantum penalizing heavy users. This is important to give fair chance to low qps
 * workloads.
 */
public class TokenSchedulerGroup extends AbstractSchedulerGroup {

  // Lifetime for which allotted token in valid. Effectively, new tokens are allotted at this frequency
  private final int tokenLifetimeMs;

  // number of tokens allotted per millisecond. 1 token corresponds to 1 millisecond of wall clock time of a thread
  // numTokensPerMs will typically correspond to the total number of threads available for execution.
  // We over-allocate total tokens by giving each group numTokensPerMs = total threads (instead of dividing
  // between two groups). This is for easy work-stealing - since each group will always have some pending tokens
  // those can be scheduled if there is no other work
  private final int numTokensPerMs;

  // currently available tokens for this group
  private int availableTokens;
  // last time token values were updated for this group
  private long lastUpdateTimeMs;
  // last time tokens were allotted for this group. Tokens are not allotted proactively after tokenLifetimeMs. Instead
  // we allot tokens in response to events - need to scheduler queries, account for threads etc.
  private long lastTokenTimeMs;
  // Internal lock for synchronizing accounting
  private final Lock tokenLock = new ReentrantLock();
  // constant factor for applying linear decay when allotting tokens.
  // We apply linear decay to temporarily lower the priority for the groups that heavily
  // used resources in the previous token cycle. Without this, groups with steady requests will
  // get a fresh start and continue to hog high resources impacting sparse users
  private static final double ALPHA = 0.80;

  TokenSchedulerGroup(String schedGroupName, int numTokensPerMs, int tokenLifetimeMs) {
    super(schedGroupName);
    Preconditions.checkArgument(numTokensPerMs > 0);
    Preconditions.checkArgument(tokenLifetimeMs > 0);
    this.numTokensPerMs = numTokensPerMs;
    this.tokenLifetimeMs = tokenLifetimeMs;
    lastUpdateTimeMs = currentTimeMillis();
    availableTokens = numTokensPerMs * tokenLifetimeMs;
    lastTokenTimeMs = lastUpdateTimeMs;
  }

  int getAvailableTokens() {
    return consumeTokens();
  }

  @Override
  public void incrementThreads() {
    consumeTokens();
    super.incrementThreads();
  }

  @Override
  public void decrementThreads() {
    consumeTokens();
    super.decrementThreads();
  }

  @Override
  public void startQuery() {
    consumeTokens();
    super.startQuery();
  }

  @Override
  public void endQuery() {
    consumeTokens();
    super.endQuery();
  }

  /**
   * Compares priority of this group with respect to another scheduler group.
   * Priority is compared on the basis of available tokens. SchedulerGroup with
   * higher number of tokens wins. If both groups have same tokens then the group
   * with earliest waiting job has higher priority (FCFS if tokens are equal).
   * If the arrival times of first waiting jobs are also equal then the group
   * with least reserved resources is selected
   * @param rhs SchedulerGroupAccount to compare with
   * @return < 0 if lhs has lower priority than rhs
   *     > 0 if lhs has higher priority than rhs
   *     = 0 if lhs has same priority as rhs
   */
  @Override
  public int compareTo(SchedulerGroupAccountant rhs) {
    if (rhs == null) {
      return 1;
    }

    if (this == rhs) {
      return 0;
    }

    int leftTokens = getAvailableTokens();
    int rightTokens = ((TokenSchedulerGroup) rhs).getAvailableTokens();
    if (leftTokens > rightTokens) {
      return 1;
    }
    if (leftTokens < rightTokens) {
      return -1;
    }
    return FCFSSchedulerGroup.compare(this, (SchedulerGroup) rhs);
  }

  public String toString() {
    return String.format(" {%s:[%d,%d,%d,%d,%d]},", name(), getAvailableTokens(), numPending(), numRunning(),
        getThreadsInUse(), totalReservedThreads());
  }

  // callers must synchronize access to this method
  private int consumeTokens() {
    try (TokenLockManager lm = new TokenLockManager(tokenLock)) {
      long currentTimeMs = currentTimeMillis();
      // multiple time qantas may have elapsed..hence, the modulo operation
      int diffMs = (int) (currentTimeMs - lastUpdateTimeMs);
      if (diffMs <= 0) {
        return availableTokens;
      }
      int threads = threadsInUse.get();
      long nextTokenTime = lastTokenTimeMs + tokenLifetimeMs;
      if (nextTokenTime > currentTimeMs) {
        availableTokens -= diffMs * threads;
      } else {
        availableTokens -= (nextTokenTime - lastUpdateTimeMs) * threads;
        // for each quantum allocate new set of tokens with linear decay of tokens.
        // Linear decay lowers the tokens available to heavy users in the next period
        // allowing light users to have better chance at scheduling. Without linear decay,
        // groups with high request rate will win more often putting light users at disadvantage.
        for (; nextTokenTime <= currentTimeMs; nextTokenTime += tokenLifetimeMs) {
          availableTokens = (int) (ALPHA * tokenLifetimeMs * numTokensPerMs
              + (1 - ALPHA) * (availableTokens - tokenLifetimeMs * threads));
        }
        lastTokenTimeMs = nextTokenTime - tokenLifetimeMs;
        availableTokens -= (currentTimeMs - lastTokenTimeMs) * threads;
      }
      lastUpdateTimeMs = currentTimeMs;
      return availableTokens;
    }
  }

  protected long currentTimeMillis() {
    return System.currentTimeMillis();
  }

  private class TokenLockManager implements AutoCloseable {
    private final Lock lock;

    TokenLockManager(Lock lock) {
      this.lock = lock;
      this.lock.lock();
    }

    @Override
    public void close() {
      lock.unlock();
    }
  }
}
