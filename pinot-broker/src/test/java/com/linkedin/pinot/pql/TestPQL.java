/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.pql;

import java.util.HashMap;

import org.json.JSONObject;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.client.request.RequestConverter;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.pql.parsers.PQLCompiler;


public class TestPQL {
  private PQLCompiler _compiler;

  @BeforeMethod
  public void before() {
    _compiler = new PQLCompiler(new HashMap<String, String[]>());
  }

  @Test
  public void simpleTestTwo() throws Exception {
    final String st4 = "select count(*) from 'xlntBeta.default' where testkey = 'lego_member-home_default_promo_slot' or testkey = 'nmp_lego_dummy_lix'";

    final JSONObject compiled = _compiler.compile(st4);

    System.out.println("****************** : " + compiled);
    // this is failing
    final BrokerRequest request = RequestConverter.fromJSON(compiled);

    System.out.println(request);

  }

  @Test
  public void simpleTestFilter() throws Exception {
    final String st4 =
        "select count(*) from 'Resource.Table' where A > 0 and B not in (111,222) and C in (333,444) and D = 555 and E < 666 and F <> 777 and G BETWEEN 888 AND 999";

    final JSONObject compiled = _compiler.compile(st4);

    System.out.println("****************** : " + compiled);
    // this is failing
    final BrokerRequest request = RequestConverter.fromJSON(compiled);

    System.out.println(request);

  }
}
