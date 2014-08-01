package com.linkedin.pinot.pql;

import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import java.util.HashMap;
import java.util.Map;

import org.antlr.runtime.RecognitionException;
import com.linkedin.pinot.pql.parsers.PQLCompiler;


public class TestPQL {

  private PQLCompiler _compiler;

  @BeforeMethod
  public void before() {
    _compiler = new PQLCompiler(new HashMap<String,String[]>());
  }

  @Test
  public void simpleTest() throws RecognitionException {
    String statement =
        "select sum(column1) where (column2 = 'value' and column3='value3') or (column3='value3' and column4='value4') "
            + "group by column6 top 100";
    System.out.println(_compiler.compile(statement).toString());
  }
}
