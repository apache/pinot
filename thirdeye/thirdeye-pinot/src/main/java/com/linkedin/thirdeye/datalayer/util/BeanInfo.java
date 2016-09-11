package com.linkedin.thirdeye.datalayer.util;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface BeanInfo {

  String tableName() default "GENERIC_JSON_ENTITY";
  
  String indexTableName();
  
  IndexInfo[] indexes() default {};
}
