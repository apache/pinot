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
    final String st4 = "select  count(*) from 'xlntBeta.default' where triggered <2 and triggered > 0";

    final JSONObject compiled = _compiler.compile(st4);

    System.out.println("****************** : " + compiled);
    // this is failing
    final BrokerRequest request = RequestConverter.fromJSON(compiled);

    System.out.println(request);

  }
}
