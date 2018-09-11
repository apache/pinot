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

package com.linkedin.thirdeye.detector.email.filter;

import com.google.common.base.MoreObjects;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AlphaBetaAlertFilter extends BaseAlertFilter {
  private final static Logger LOG = LoggerFactory.getLogger(AlphaBetaAlertFilter.class);

  // These default parameters are accessed through Java reflection. Do not remove.
  public static final String DEFAULT_ALPHA = "1";
  public static final String DEFAULT_BETA = "1";
  public static final String DEFAULT_THRESHOLD = "0.8";
  public static final String DEFAULT_TYPE = "alpha_beta";

  public static final String ALPHA = "alpha";
  public static final String BETA = "beta";
  public static final String THRESHOLD = "threshold";
  public static final String TYPE = "type";

  private double alpha = Double.parseDouble(DEFAULT_ALPHA);
  private double beta = Double.parseDouble(DEFAULT_BETA);
  private double threshold = Double.parseDouble(DEFAULT_THRESHOLD);
  private String type = DEFAULT_TYPE;

  private static final List<String> propertyNames =
      Collections.unmodifiableList(new ArrayList<>(Arrays.asList(ALPHA, BETA, THRESHOLD, TYPE)));

  public List<String> getPropertyNames() {
    return propertyNames;
  }

  public AlphaBetaAlertFilter() {

  }

  public AlphaBetaAlertFilter(double alpha, double beta, double threshold) {
    this.alpha = alpha;
    this.beta = beta;
    this.threshold = threshold;
  }

  public double getAlpha() {
    return alpha;
  }

  public void setAlpha(double alpha) {
    this.alpha = alpha;
  }

  public double getBeta() {
    return beta;
  }

  public void setBeta(double beta) {
    this.beta = beta;
  }

  public double getThreshold() {
    return threshold;
  }

  public void setThreshold(double threshold) {
    this.threshold = threshold;
  }

  public String getType() {
    return this.type;
  }

  @Override
  public boolean isQualified(MergedAnomalyResultDTO anomaly) {
    double lengthInHour =
        (double) (anomaly.getEndTime() - anomaly.getStartTime()) / 36_00_000d;
    // In ThirdEye, the absolute value of weight is the severity
    double qualificationScore =
        Math.pow(lengthInHour, alpha) * Math.pow(Math.abs(anomaly.getWeight()), beta);
    return (qualificationScore > threshold);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add(ALPHA, alpha).add(BETA, beta).add(THRESHOLD, threshold).add(TYPE, type).toString();
  }
}
