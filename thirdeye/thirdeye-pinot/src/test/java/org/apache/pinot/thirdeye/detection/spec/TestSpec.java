/*
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

package org.apache.pinot.thirdeye.detection.spec;

import org.apache.pinot.thirdeye.detection.components.RuleBaselineProvider;
import java.util.Map;


public class TestSpec extends AbstractSpec{
  private int a = 123;
  private double b = 456.7;
  private String c = "default";
  private RuleBaselineProvider baselineProvider;
  private Map<String, String> configuration;
  private double threshold = 0.1;
  private double upThreshold;
  private double downThreshold;

  public Map<String, String> getConfiguration() {
    return configuration;
  }

  public void setConfiguration(Map<String, String> configuration) {
    this.configuration = configuration;
  }

  public RuleBaselineProvider getBaselineProvider() {
    return baselineProvider;
  }

  public void setBaselineProvider(RuleBaselineProvider baselineProvider) {
    this.baselineProvider = baselineProvider;
  }

  public int getA() {
    return a;
  }

  public void setA(int a) {
    this.a = a;
  }

  public double getB() {
    return b;
  }

  public void setB(double b) {
    this.b = b;
  }

  public String getC() {
    return c;
  }

  public void setC(String c) {
    this.c = c;
  }

  public double getThreshold() {
    return threshold;
  }

  public void setThreshold(double threshold) {
    this.threshold = threshold;
  }

  public double getUpThreshold() {
    return upThreshold;
  }

  public void setUpThreshold(double upThreshold) {
    this.upThreshold = upThreshold;
  }

  public double getDownThreshold() {
    return downThreshold;
  }

  public void setDownThreshold(double downThreshold) {
    this.downThreshold = downThreshold;
  }
}

