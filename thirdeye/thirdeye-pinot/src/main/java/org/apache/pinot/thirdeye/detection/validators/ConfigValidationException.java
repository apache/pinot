/*
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

package org.apache.pinot.thirdeye.detection.validators;

import com.github.fge.jsonschema.core.report.ProcessingReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Thrown if any requirement from the {@link ConfigValidator} was violated.
 */
public class ConfigValidationException extends Exception {
  protected static final Logger LOG = LoggerFactory.getLogger(ConfigValidationException.class);

  private final ProcessingReport report;

  public ConfigValidationException(String message, ProcessingReport report, Throwable e) {
    super(message, e);
    this.report = report;
  }

  public ConfigValidationException(ProcessingReport report) {
    this(ConfigValidationUtils.getValidationMessage(report), report, null);
  }

  public ConfigValidationException(String message) {
    this(message, null, null);
  }

  /**
   * Returns the complete validation report along with the message
   */
  public String getValidationErrorReport() {
    if (report == null) {
      return "No Report";
    }
    return report.toString();
  }
}
