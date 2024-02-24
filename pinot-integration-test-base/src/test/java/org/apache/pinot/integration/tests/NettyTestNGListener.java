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
package org.apache.pinot.integration.tests;

import io.netty.buffer.ByteBufUtil;
import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.ITestContext;
import org.testng.ITestResult;


public class NettyTestNGListener implements IInvokedMethodListener {
  private static final NettyLeakListener NETTY_LEAK_LISTENER;
  private static final String LEAK_DETECTION_LEVEL_PROP_KEY = "io.netty.leakDetection.level";

  static {
    if (System.getProperty(LEAK_DETECTION_LEVEL_PROP_KEY) == null) {
      System.setProperty(LEAK_DETECTION_LEVEL_PROP_KEY, "paranoid");
    }
    NETTY_LEAK_LISTENER = new NettyLeakListener();
    ByteBufUtil.setLeakListener(NETTY_LEAK_LISTENER);
  }

  @Override
  public void beforeInvocation(IInvokedMethod method, ITestResult testResult, ITestContext context) {
    NETTY_LEAK_LISTENER.assertZeroLeaks();
  }

  @Override
  public void afterInvocation(IInvokedMethod method, ITestResult testResult, ITestContext context) {
    NETTY_LEAK_LISTENER.assertZeroLeaks();
  }
}
