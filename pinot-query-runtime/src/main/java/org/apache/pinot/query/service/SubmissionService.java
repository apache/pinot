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
package org.apache.pinot.query.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Submission service is used to submit multiple runnables and checks the result upon all {@link Future} returns
 * or any failure occurs.
 */
public class SubmissionService {
  private static final Logger LOGGER = LoggerFactory.getLogger(SubmissionService.class);

  private final ExecutorService _executor;
  private final List<CompletableFuture<Void>> _futures = new ArrayList<>();

  public SubmissionService(ExecutorService executor) {
    _executor = executor;
  }

  public void submit(Runnable runnable) {
    _futures.add(CompletableFuture.runAsync(runnable, _executor));
  }

  public void awaitFinish(long deadlineMs) throws ExecutionException {
    CompletableFuture<Void> completableFuture = CompletableFuture.allOf(_futures.toArray(new CompletableFuture[]{}));
    try {
      completableFuture.get(deadlineMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    } catch (Throwable t) {
      LOGGER.error("error occurred during submission", t);
      throw new ExecutionException("error occurred during submission", t);
    } finally {
      // Cancel all ongoing submission
      for (CompletableFuture<Void> future : _futures) {
        if (!future.isDone()) {
          future.cancel(true);
        }
      }
    }
  }
}
