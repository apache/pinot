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
  public void simpleTest() throws Exception {
    final String st4 = "select min(column) from x where c1 = 'v1' and c2='v2' and c3='v3' order by y limit 10,10 ";

    final JSONObject compiled = _compiler.compile(st4);

    // this is failing
    final BrokerRequest request = RequestConverter.fromJSON(compiled);
    System.out.println(request);
  }
}
