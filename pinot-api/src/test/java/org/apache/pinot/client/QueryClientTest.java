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
package org.apache.pinot.client;

import org.testng.annotations.Test;


/**
 *  Tests for resp body java client api coding utf-8
 */
public class QueryClientTest {
  @Test
  public void ClientRespCoding() {
    Connection connection =
        ConnectionFactory.fromHostList("10.85.126.103:8099", "10.85.126.104:8099", "10.85.126.105:8099");
    ResultSetGroup resultSetGroup =
        connection.execute("select city, country, province, manufacturer, play_count from video-qoe "
            + "where uid='5305426956' limit 10");
    System.out.println(resultSetGroup);

    /*
      There is no uniform utf-8, query non-english fields have garbled
      city         country      province     manufacturer play_count
      ==========================================================
      éå·       ä¸­å½       æ²³å       iPhone8      3
      éå·       ä¸­å½       æ²³å       iPhone8      3
      null         ä¸­å½       æ²³å       iPhone8      1
      éå·       ä¸­å½       æ²³å       iPhone8      34
      éå·       ä¸­å½       æ²³å       iPhone8      2
      null         ä¸­å½       æ²³å       iPhone8      1
      éå·       ä¸­å½       æ²³å       iPhone8      1
      éå·       ä¸­å½       æ²³å       iPhone8      16
      éå·       ä¸­å½       æ²³å       iPhone8      1
      éå·       ä¸­å½       æ²³å       iPhone8      1
    */

    /*
      Unified as utf-8, fixed garbled non-english fields
      city   country province manufacturer play_count
      ===========================================
      郑州     中国      河南       iPhone8      3
      郑州     中国      河南       iPhone8      3
      郑州     中国      河南       iPhone8      34
      郑州     中国      河南       iPhone8      2
      null   中国      河南       iPhone8      1
      null   中国      河南       iPhone8      1
      郑州     中国      河南       iPhone8      16
      郑州     中国      河南       iPhone8      1
      郑州     中国      河南       iPhone8      1
      郑州     中国      河南       iPhone8      15
     */
  }
}

