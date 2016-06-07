package com.linkedin.pinot.common.restlet.swagger;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;


/**
 * Documents the responses that can be returned by an API call.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Responses {
  Response[] value();
}
