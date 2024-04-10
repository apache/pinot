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
package org.apache.pinot.segment.local.utils;

import org.apache.commons.codec.binary.Base64;

/**
 * Simple wrapper class over codec's Base64 implementation to handle Pinot-specific Base64 encoded binary data.
 */
public class Base64Utils extends Base64 {
  public static boolean isBase64IgnoreWhiteSpace(byte[] arrayOctet) {
    return isBase64(arrayOctet);
  }

  public static boolean isBase64IgnoreTrailingPeriods(byte[] arrayOctet) {
    int i = arrayOctet.length - 1;
    while (i >= 0 && '.' == arrayOctet[i]) {
      --i;
    }
    while (i >= 0) {
      if (!isBase64(arrayOctet[i])) {
        return false;
      }
      --i;
    }
    return true;
  }
}
