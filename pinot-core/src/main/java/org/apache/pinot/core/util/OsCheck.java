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
package org.apache.pinot.core.util;

import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class OsCheck {

  // cached result of OS detection
  private static final OSType _detectedOS;

  private OsCheck() {
  }

  /**
   * detect the operating system from the os.name System property and cache
   * the result
   *
   * @return - the operating system detected
   */
  public static OSType getOperatingSystemType() {
    return _detectedOS;
  }

  /**
   * types of Operating Systems
   */
  public enum OSType {
    Windows, MacOS, Linux, Other
  }

  private static final Logger log = LoggerFactory.getLogger(OsCheck.class);

  static {
    String os = System.getProperty("os.name", "generic").toLowerCase(Locale.ENGLISH);
    log.info("System property \"os.name\" is [{}]", os);
    if ((os.contains("mac")) || (os.contains("darwin"))) {
      _detectedOS = OSType.MacOS;
    } else if (os.contains("win")) {
      _detectedOS = OSType.Windows;
    } else if (os.contains("linux")) {
      _detectedOS = OSType.Linux;
    } else {
      _detectedOS = OSType.Other;
    }
  }
}
