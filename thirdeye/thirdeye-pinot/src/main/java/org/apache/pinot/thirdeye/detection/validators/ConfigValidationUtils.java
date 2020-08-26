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

import com.github.fge.jsonschema.core.report.ProcessingMessage;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import java.util.Iterator;

import static org.apache.pinot.thirdeye.detection.validators.ThirdEyeUserConfigValidator.*;


/**
 * Holds utility methods related to ThirdEye user config validation
 */
public class ConfigValidationUtils {

  /**
   * Parses and extracts the validation error message from the report
   */
  public static String getValidationMessage(ProcessingReport report) {
    if (report == null) {
      return "";
    }
    final StringBuilder builder = new StringBuilder(USER_CONFIG_VALIDATION_FAILED_KEY);
    Iterator<ProcessingMessage> reportIt = report.iterator();
    while (!report.isSuccess() && reportIt.hasNext()) {
      builder.append("\n- ").append(reportIt.next().getMessage());
    }
    return builder.toString();
  }

  /**
   * Similar to @{link Preconditions.checkArgument} but throws a {@link ConfigValidationException}
   */
  public static void checkArgument(boolean expression, Object errorMessage) throws ConfigValidationException {
    if (!expression) {
      throw new ConfigValidationException(String.valueOf(errorMessage));
    }
  }
}
