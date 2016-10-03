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
package com.linkedin.pinot.query.transform;

import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.common.request.transform.TransformFunction;
import com.linkedin.pinot.common.request.transform.TransformFunctionFactory;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import com.linkedin.pinot.pql.parsers.pql2.ast.AstNode;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for {@link TransformExpressionTree} class.
 */
public class TransformExpressionTreeTest {

  /**
   * This test validates an expression tree built by {@link TransformExpressionTree#buildTree(AstNode)}
   */
  @Test
  public void test() {
    TransformFunctionFactory.init(
        new String[]{TransformFunctionFactoryTest.foo.class.getName(), TransformFunctionFactoryTest.bar.class.getName()});
    Pql2Compiler compiler = new Pql2Compiler();

    String expression = "foo(bar('a', foo(b, 'c', d)), e)";
    TransformExpressionTree expressionTree = compiler.compileToExpressionTree(expression);

    TransformFunction rootTransform = expressionTree.getTransform();
    Assert.assertEquals(rootTransform.getName(), "foo");

    List<TransformExpressionTree> firstChildren = expressionTree.getChildren();
    Assert.assertEquals(firstChildren.size(), 2);

    TransformExpressionTree firstChild = firstChildren.get(0);
    Assert.assertEquals(firstChild.getTransform().getName(), "bar");
    Assert.assertEquals(firstChildren.get(1).getColumn(), "e");

    List<TransformExpressionTree> secondChildren = firstChild.getChildren();
    Assert.assertEquals(secondChildren.size(), 2);
    Assert.assertEquals(secondChildren.get(0).getLiteral(), "a");
    Assert.assertEquals(secondChildren.get(1).getTransform().getName(), "foo");

    List<TransformExpressionTree> thirdChildren = secondChildren.get(1).getChildren();
    Assert.assertEquals(thirdChildren.get(0).getColumn(), "b");
    Assert.assertEquals(thirdChildren.get(1).getLiteral(), "c");
    Assert.assertEquals(thirdChildren.get(2).getColumn(), "d");
  }
}
