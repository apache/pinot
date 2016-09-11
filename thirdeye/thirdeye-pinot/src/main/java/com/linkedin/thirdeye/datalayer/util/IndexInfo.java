package com.linkedin.thirdeye.datalayer.util;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface IndexInfo {

  String fieldName();
  
  String columnName();
}
