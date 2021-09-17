package org.apache.pinot.common.function.scalar;

/**
 * Interface that defines {@link org.apache.pinot.spi.annotations.ScalarFunction}
 * annotated Function that supports preprocess literal arguments.
 */
public interface InitializableScalarFunction {

  /**
   * Initialize internal state. This will be called by the query expression parser
   * during expression parsing and called with argument.
   *
   * Each concrete implementation should check and validate the desired positional
   * arguments and ensure that it is of literal type.
   *
   * @param arguments positional argument expressions.
   */
  void init(Object... arguments);
}
