package com.linkedin.pinot.integration.tests.helpers;

import com.linkedin.pinot.common.data.FieldSpec.DataType;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 13, 2014
 */

public class GeneratorFactory {

  public static Class getGeneratorFor(DataType type) {

    if (type == DataType.STRING) {
      return StringGenerator.class;
    }

    return NumberGenerator.class;
  }

  public static Generator getGeneratorFor(DataType type, int cardinality) {
    if (type == DataType.STRING) {
      return new StringGenerator(cardinality);
    }

    return new NumberGenerator(cardinality, type, false);
  }
}
