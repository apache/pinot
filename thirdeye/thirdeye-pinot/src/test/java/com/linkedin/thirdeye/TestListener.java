package com.linkedin.thirdeye;

import com.linkedin.thirdeye.datasource.DAORegistry;

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
