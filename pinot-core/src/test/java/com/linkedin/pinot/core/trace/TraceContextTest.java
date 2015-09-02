/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.trace;

import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.utils.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

import java.util.*;
import java.util.concurrent.*;

/**
 * Mimic the behavior of server-side multi-threading, and test whether the log maintains multi-threading invariants.
 *
 */
public class TraceContextTest {

  private static final String SEP = "_";
  private static final Logger LOGGER = LoggerFactory.getLogger(TraceContextTest.class);

  static final int JOBS_PER_REQUEST = 100;
  static final int NUMBER_OF_REQUESTS = 50;
  static final int NUMBER_OF_THREADS = 50;

  /**
   * CountInfo is a mutable structure.
   */
  private static class CountInfo {
    public final Set<Long> numberOfRequests = new HashSet<Long>();
    public final Map<Long, Integer> numberOfJobsMap = new HashMap<Long, Integer>();

    private final int requestsNum;
    private final int jobsNumPerRequest;

    public CountInfo(int requestsNum, int jobsNumPerRequest) {
      this.requestsNum = requestsNum;
      this.jobsNumPerRequest = jobsNumPerRequest;
    }

    public void verify() {
      assertEquals(numberOfRequests.size(), requestsNum);
      assertEquals(numberOfJobsMap.keySet().size(), requestsNum);
      for (int num: numberOfJobsMap.values()) {
        assertEquals(num, jobsNumPerRequest);
      }
    }
  }

  // ------ helper functions ------

  private void mixedInvariants(ConcurrentLinkedDeque<TraceContext.Info> queue, CountInfo countInfo) {
    while (queue.size() > 0) {
      if (queue.peekFirst().message == TraceContext.CONSTANT.REGISTER_REQUEST) {
        requestHandlerInvariants(queue, countInfo);
      } else if (queue.peekFirst().message == TraceContext.CONSTANT.REGISTER_THREAD_TO_REQUEST) {
        jobHandlerInvariants(queue, countInfo);
      } else {
        throw new RuntimeException("Remaining Queue malformed: " + queue);
      }
    }
  }

  private void requestHandlerInvariants(ConcurrentLinkedDeque<TraceContext.Info> queue, CountInfo countInfo) {
    TraceContext.Info info1 = queue.pollFirst();
    TraceContext.Info info2 = queue.pollFirst();
    TraceContext.Info info3 = queue.pollFirst();
    TraceContext.Info info4 = queue.pollFirst();
    assertEquals(info1.requestId, info2.requestId);
    assertEquals(info2.requestId, info3.requestId);
    assertEquals(info3.requestId, info4.requestId);
    assertEquals(info1.message, TraceContext.CONSTANT.REGISTER_REQUEST);
    assertEquals(info2.message, TraceContext.CONSTANT.REGISTER_THREAD_TO_REQUEST);
    assertEquals(info3.message, TraceContext.CONSTANT.UNREGISTER_THREAD_FROM_REQUEST);
    assertEquals(info4.message, TraceContext.CONSTANT.UNREGISTER_REQUEST);

    countInfo.numberOfRequests.add(info1.requestId);
  }

  private void putIfAbsent(Map<Long, Integer> map, Long key, Integer value) {
    Integer v = map.get(key);
    if (v == null) {
      map.put(key, value);
    }
  }

  private void jobHandlerInvariants(ConcurrentLinkedDeque<TraceContext.Info> queue, CountInfo countInfo) {
    TraceContext.Info info1 = queue.pollFirst();
    TraceContext.Info info2 = queue.pollFirst();
    TraceContext.Info info3 = queue.pollFirst();
    assertEquals(info1.requestId, info2.requestId);
    assertEquals(info2.requestId, info3.requestId);
    assertEquals(info1.message, TraceContext.CONSTANT.REGISTER_THREAD_TO_REQUEST);
    assertEquals(info2.message, TraceContext.CONSTANT.START_NEW_TRACE);
    assertEquals(info3.message, TraceContext.CONSTANT.UNREGISTER_THREAD_FROM_REQUEST);

    putIfAbsent(countInfo.numberOfJobsMap, info1.requestId, 0);
    countInfo.numberOfJobsMap.put(info1.requestId, countInfo.numberOfJobsMap.get(info1.requestId) + 1);
  }

  private void singleRequestCall(ExecutorService jobService, final InstanceRequest request, int jobsPerRequest) throws Exception {
    TraceContext.register(request);
    Future[] tasks = new Future[jobsPerRequest];

    // fan out multiple threads for this request
    for (int i = 0; i < jobsPerRequest; i++) {
      final int taskId = i;
      tasks[i] = jobService.submit(new TraceRunnable() {
        @Override
        public void runJob() {
          String tid = Thread.currentThread().getId() + "";
          TraceContext.log(tid, request.getRequestId() + SEP + taskId);
        }
      });
    }

    // wait for all threads to finish the job
    for (int i = 0; i < jobsPerRequest; i++) {
      // block waiting
      tasks[i].get();
    }
    TraceContext.unregister(request);
  }

  /**
   * The {@link ExecutorService} for handling requests can be
   * 1. the same service that handling internal multi-threading jobs of each request
   * 2. or a different service
   *
   * @param requestService
   * @throws Exception
   */
  private void multipleRequestsCall(ExecutorService requestService, final ExecutorService jobService,
                                    int numberOfRequests, final int jobsPerRequest) throws Exception {
    // init requests
    List<InstanceRequest> requests = new ArrayList<InstanceRequest>();
    for (int j = 0; j < numberOfRequests; j++) {
      InstanceRequest request = new InstanceRequest(j, null);
      request.setEnableTrace(true);
      requests.add(request);
    }

    // init tasks
    Future[] tasks = new Future[requests.size()];
    int i = 0;
    for (final InstanceRequest request: requests) {
      tasks[i++] = requestService.submit(new Runnable() {
        @Override
        public void run() {
          try {
            singleRequestCall(jobService, request, jobsPerRequest);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      });
    }

    // wait for all tasks to finish
    for (Future task: tasks) {
      // block waiting
      task.get();
    }
  }

  private void runVariousConditionTests(ExecutorService requestService, ExecutorService jobService) throws Exception {
    int reqStep = NUMBER_OF_REQUESTS/5;
    int jobStep = JOBS_PER_REQUEST/5;
    boolean shared = false;

    if (requestService == jobService) {
      shared = true;
    }

    for (int numberOfRequests = reqStep; numberOfRequests <= NUMBER_OF_REQUESTS; numberOfRequests += reqStep) {
      for (int jobsPerRequest = jobStep; jobsPerRequest < JOBS_PER_REQUEST; jobsPerRequest += jobStep) {
        if (shared) {
          LOGGER.info("Shared exec service for #request: " + numberOfRequests + " #jobsPerRequest: " + jobsPerRequest);
        } else {
          LOGGER.info("Non-shared exec service for #request: " + numberOfRequests + " #jobsPerRequest: " + jobsPerRequest);
        }

        if (shared && (numberOfRequests>=NUMBER_OF_THREADS)) {
          LOGGER.info("Ignore tests for #request: " + numberOfRequests + ", since it will deadlock.");
        } else {
          TraceContext.reset();
          multipleRequestsCall(requestService, jobService, numberOfRequests, jobsPerRequest);
          runVerification(numberOfRequests, jobsPerRequest);
        }
      }
    }
  }

  private void runVerification(int numberOfRequests, int jobsPerRequest) {
    // verify control info
    CountInfo countInfo = new CountInfo(numberOfRequests, jobsPerRequest);
    ConcurrentHashMap<Long, ConcurrentLinkedDeque<TraceContext.Info>> testInfoMap = TraceContext.getTestInfoMap();

    // LOGGER.info("Thread {}: {}", key, testInfoMap.get(key).toString().replace(",", "\n"));
    for (ConcurrentLinkedDeque<TraceContext.Info> queue : testInfoMap.values()) {
      mixedInvariants(queue, countInfo);
    }

    countInfo.verify();

    // verify trace info
    for (long rqId = 0; rqId < numberOfRequests; rqId++) {
      ConcurrentLinkedDeque<Trace> traceQueue = TraceContext.getAllTraceInfoMap().get(rqId);
      assertEquals(traceQueue.size(), jobsPerRequest + 1);
      Set<Integer> jobIdSet = new HashSet<Integer>();
      for (Trace trace: traceQueue) {
        // one trace is used for request handler, it has no info recorded
        if (trace.getValue().size() > 0) {
          Object obj = trace.getValue().get(0); // we have recorded one entry per job in tests
          String[] tmp = ((String) obj).split(SEP);
          long reqId = Long.parseLong(tmp[0].trim());
          assertEquals(rqId, reqId);
          int jobId = Integer.parseInt(tmp[1].trim());
          jobIdSet.add(jobId);
        }
      }
      assertEquals(jobIdSet.size(), jobsPerRequest);
      // show trace
      LOGGER.info("Trace Tree: {}", TraceContext.getTraceInfoOfRequestId(rqId));
    }
  }

  // ------ test functions ------

  @BeforeClass
  public static void before() throws Exception {
    TraceContext.TEST_ENABLED = true;
  }

  @AfterClass
  public static void after() throws Exception {
    TraceContext.TEST_ENABLED = false;
  }

  @Test
  public void testSingleRequest() throws Exception {
    TraceContext.reset();
    ExecutorService jobService = Executors.newFixedThreadPool(NUMBER_OF_THREADS, new NamedThreadFactory("jobService"));
    InstanceRequest request = new InstanceRequest(0L, null);
    request.setEnableTrace(true);
    singleRequestCall(jobService, request, JOBS_PER_REQUEST);
    runVerification(1, JOBS_PER_REQUEST);
  }

  /**
   * If using the shared {@link ExecutorService} created by {@link Executors#newFixedThreadPool(int)}
   * for handling requests and jobs, avoiding potential deadlock by making sure NUMBER_OF_REQUESTS &lt; nThreads.
   * (Using non-shared {@link ExecutorService} is no issue.)
   *
   * @throws Exception
   */
  @Test
  public void testMultipleRequests() throws Exception {
    TraceContext.reset();

    // shared Service
    ExecutorService sharedService = Executors.newFixedThreadPool(NUMBER_OF_THREADS, new NamedThreadFactory("sharedService"));
    runVariousConditionTests(sharedService, sharedService);

    // non-shared Service
    ExecutorService requestService = Executors.newFixedThreadPool(NUMBER_OF_THREADS, new NamedThreadFactory("requestService"));
    ExecutorService jobService = Executors.newFixedThreadPool(NUMBER_OF_THREADS, new NamedThreadFactory("jobService"));
    runVariousConditionTests(requestService, jobService);
  }

}