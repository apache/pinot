/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package org.apache.pinot.thirdeye;

import org.apache.pinot.thirdeye.datasource.DAORegistry;

import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

public class TestListener implements ITestListener {

  @Override
  public void onTestStart(ITestResult iTestResult) {
    System.out.println("*** Running %s.%s() ***".format(iTestResult.getInstanceName(), iTestResult.getMethod().getMethodName()));
  }

  @Override
  public void onTestSuccess(ITestResult iTestResult) {
    // blank
  }

  @Override
  public void onTestFailure(ITestResult iTestResult) {
    // blank
  }

  @Override
  public void onTestSkipped(ITestResult iTestResult) {
    // blank
  }

  @Override
  public void onTestFailedButWithinSuccessPercentage(ITestResult iTestResult) {
    // blank
  }

  @Override
  public void onStart(ITestContext iTestContext) {
    // blank
  }

  @Override
  public void onFinish(ITestContext iTestContext) {
    // blank
  }
}
