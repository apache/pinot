/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.utils;

import org.apache.commons.httpclient.params.DefaultHttpParams;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotMinionUserAgentHeader {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotMinionUserAgentHeader.class);

  public static String getTaskType(String userAgentHeader) {
    try {
      return StringUtils.substringsBetween(userAgentHeader, CommonConstants.Minion.MINION_HEADER_PREFIX, CommonConstants.Minion.MINION_HEADER_SEPARATOR)[0];
    } catch (NullPointerException e) {
      LOGGER.info("Could not get task type for userAgentHeader {}", userAgentHeader);
      throw new RuntimeException(e);
    }
  }

  public static String constructUserAgentHeader(String taskType, String minionVersion) {
    String minionUserAgentParameter =
        CommonConstants.Minion.HTTP_TASK_TYPE_HEADER_PREFIX + taskType + CommonConstants.Minion.MINION_HEADER_SEPARATOR + minionVersion;
    String defaultUserAgentParameter =
        DefaultHttpParams.getDefaultParams().getParameter(HttpMethodParams.USER_AGENT).toString();
    return defaultUserAgentParameter + " " + minionUserAgentParameter;
  }
}
