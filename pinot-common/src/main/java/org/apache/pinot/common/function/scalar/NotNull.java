package org.apache.pinot.common.function.scalar;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/***
 * This annotation is an explicit contract that an inbuilt function
 * cannot handle a null value of the annotated parameter.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface NotNull {
  String message() default "Inbuilt function cannot handle a null value of the parameter.";
}
