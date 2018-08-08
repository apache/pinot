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
package com.linkedin.pinot.common.utils;

import java.util.Enumeration;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class LogUtils {
  public static void setLogLevel(List<String> packagePrefixes, Level level) {
    Enumeration<Logger> loggers = Logger.getRootLogger().getLoggerRepository().getCurrentLoggers();
    while (loggers.hasMoreElements()) {
      Logger logger = loggers.nextElement();
      for (String prefix : packagePrefixes) {
        if (logger.getName().startsWith(prefix)) {
          logger.setLevel(level);
          break;
        }
      }
    }
  }
}
