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
package org.apache.pinot.common.request.transform;

import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for {@link TransformExpressionTree} class.
 */
public class TransformExpressionTreeTest {
  private static final String STANDARD_EXPRESSION = "foo(bar('a',foobar(b,'c','123')),d)";
  private static final TransformExpressionTree STANDARD_EXPRESSION_TREE =
      TransformExpressionTree.compileToExpressionTree(STANDARD_EXPRESSION);

  @Test
  public void testConstructor() {
    Assert.assertEquals(STANDARD_EXPRESSION_TREE.toString(), STANDARD_EXPRESSION);
    Assert.assertEquals(STANDARD_EXPRESSION_TREE.getExpressionType(), TransformExpressionTree.ExpressionType.FUNCTION);
    Assert.assertEquals(STANDARD_EXPRESSION_TREE.getValue(), "foo");

    List<TransformExpressionTree> fooChildren = STANDARD_EXPRESSION_TREE.getChildren();
    Assert.assertEquals(fooChildren.size(), 2);
    TransformExpressionTree fooChild0 = fooChildren.get(0);
    Assert.assertEquals(fooChild0.getExpressionType(), TransformExpressionTree.ExpressionType.FUNCTION);
    Assert.assertEquals(fooChild0.getValue(), "bar");
    TransformExpressionTree fooChild1 = fooChildren.get(1);
    Assert.assertEquals(fooChild1.getExpressionType(), TransformExpressionTree.ExpressionType.IDENTIFIER);
    Assert.assertEquals(fooChild1.getValue(), "d");
    Assert.assertNull(fooChild1.getChildren());

    List<TransformExpressionTree> barChildren = fooChild0.getChildren();
    Assert.assertEquals(barChildren.size(), 2);
    TransformExpressionTree barChild0 = barChildren.get(0);
    Assert.assertEquals(barChild0.getExpressionType(), TransformExpressionTree.ExpressionType.LITERAL);
    Assert.assertEquals(barChild0.getValue(), "a");
    Assert.assertNull(barChild0.getChildren());
    TransformExpressionTree barChild1 = barChildren.get(1);
    Assert.assertEquals(barChild1.getExpressionType(), TransformExpressionTree.ExpressionType.FUNCTION);
    Assert.assertEquals(barChild1.getValue(), "foobar");

    List<TransformExpressionTree> foobarChildren = barChild1.getChildren();
    Assert.assertEquals(foobarChildren.size(), 3);
    TransformExpressionTree foobarChild0 = foobarChildren.get(0);
    Assert.assertEquals(foobarChild0.getExpressionType(), TransformExpressionTree.ExpressionType.IDENTIFIER);
    Assert.assertEquals(foobarChild0.getValue(), "b");
    Assert.assertNull(foobarChild0.getChildren());
    TransformExpressionTree foobarChild1 = foobarChildren.get(1);
    Assert.assertEquals(foobarChild1.getExpressionType(), TransformExpressionTree.ExpressionType.LITERAL);
    Assert.assertEquals(foobarChild1.getValue(), "c");
    Assert.assertNull(foobarChild1.getChildren());
    TransformExpressionTree foobarChild2 = foobarChildren.get(2);
    Assert.assertEquals(foobarChild2.getExpressionType(), TransformExpressionTree.ExpressionType.LITERAL);
    Assert.assertEquals(foobarChild2.getValue(), "123");
    Assert.assertNull(foobarChild2.getChildren());
  }

  @Test
  public void testWhiteSpace() {
    String expression = "  \t  foo\t  ( bar   ('a'\t ,foobar(    b,   'c'  \t, '123')  )   ,d  )\t";
    Assert.assertTrue(equalsWithStandardExpressionTree(TransformExpressionTree.compileToExpressionTree(expression)));

    expression = "foo(bar(' a',foobar(b,'c','123')),d)";
    Assert.assertFalse(equalsWithStandardExpressionTree(TransformExpressionTree.compileToExpressionTree(expression)));

    expression = "foo(bar('a',foobar(b,'c\t','123')),d)";
    Assert.assertFalse(equalsWithStandardExpressionTree(TransformExpressionTree.compileToExpressionTree(expression)));
  }

  @Test
  public void testQuoteOnLiteral() {
    String expression = "foo(bar('a',foobar(b,'c',123)),d)";
    Assert.assertTrue(equalsWithStandardExpressionTree(TransformExpressionTree.compileToExpressionTree(expression)));
    expression = "foo(bar(\"a\",foobar(b,\"c\",\"123\")),d)";
    Assert.assertTrue(equalsWithStandardExpressionTree(TransformExpressionTree.compileToExpressionTree(expression)));
  }

  @Test
  public void testEscapeQuotes() {
    String expression = "foo('a''b')";
    TransformExpressionTree expressionTree = TransformExpressionTree.compileToExpressionTree(expression);
    Assert.assertEquals(expressionTree.getChildren().get(0),
        new TransformExpressionTree(TransformExpressionTree.ExpressionType.LITERAL, "a'b", null));
    Assert.assertEquals(TransformExpressionTree.compileToExpressionTree(expressionTree.toString()), expressionTree);
  }

  @Test
  public void testUpperCase() {
    String expression = "foO(bAr('a',FOoBar(b,'c',123)),d)";
    Assert.assertTrue(equalsWithStandardExpressionTree(TransformExpressionTree.compileToExpressionTree(expression)));
  }

  @Test
  public void testNoArgFunction() {
    String expression = "now()";
    TransformExpressionTree expressionTree = TransformExpressionTree.compileToExpressionTree(expression);
    Assert.assertTrue(expressionTree.isFunction());
    Assert.assertEquals(expressionTree.getValue(), "now");
    Assert.assertEquals(expressionTree.getChildren().size(), 0);
  }

  private static boolean equalsWithStandardExpressionTree(TransformExpressionTree expressionTree) {
    return expressionTree.hashCode() == STANDARD_EXPRESSION_TREE.hashCode() && expressionTree.equals(STANDARD_EXPRESSION_TREE)
        && expressionTree.toString().equals(STANDARD_EXPRESSION);
  }
}
