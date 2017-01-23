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

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.operator.transform.function.TransformFunction;
import com.linkedin.pinot.core.operator.transform.function.TransformFunctionFactory;
import junit.framework.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for TransformFunctionFactory class.
 */
public class TransformFunctionFactoryTest {

  /**
   * This test ensure that any transform class can be loaded as long as it is
   * in the class path.
   */
  @Test
  public void testTransformLoading() {
    TransformFunction[] transforms = new TransformFunction[]{new foo(), new bar(), new foo()};
    String[] classNames = new String[transforms.length];

    for (int i = 0; i < transforms.length; i++) {
      classNames[i] = transforms[i].getClass().getName();
    }
    TransformFunctionFactory.init(classNames);

    // Assert that the transform factory can provide instances for the transforms.
    for (TransformFunction transform : transforms) {
      Assert.assertNotNull(TransformFunctionFactory.get(transform.getName()));
    }
  }

  /**
   * Dummy class to be instantiated for testing loading of transforms.
   */
  public static class foo implements TransformFunction {

    @Override
    public <T> T transform(int length, BlockValSet... input) {
      return null;
    }

    @Override
    public FieldSpec.DataType getOutputType() {
      return null;
    }

    @Override
    public String getName() {
      return "foo";
    }
  }

  /**
   * Dummy class to be instantiated for testing loading of transforms.
   */
  public static class bar implements TransformFunction {

    @Override
    public <T> T transform(int length, BlockValSet... input) {
      return null;
    }

    @Override
    public FieldSpec.DataType getOutputType() {
      return null;
    }

    @Override
    public String getName() {
      return "bar";
    }
  }
}
