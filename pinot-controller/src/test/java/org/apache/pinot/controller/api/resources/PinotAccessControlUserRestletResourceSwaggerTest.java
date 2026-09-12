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
package org.apache.pinot.controller.api.resources;

import io.swagger.annotations.ApiParam;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import javax.ws.rs.QueryParam;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class PinotAccessControlUserRestletResourceSwaggerTest {

  @Test
  public void testUserComponentQueryParamIsRequired()
      throws Exception {
    assertComponentQueryParamRequired(
        PinotAccessControlUserRestletResource.class.getMethod("getUser", String.class, String.class));
    assertComponentQueryParamRequired(
        PinotAccessControlUserRestletResource.class.getMethod("deleteUser", String.class, String.class));
    assertComponentQueryParamRequired(
        PinotAccessControlUserRestletResource.class.getMethod("updateUserConfig", String.class, String.class,
            boolean.class, String.class));
  }

  private static void assertComponentQueryParamRequired(Method method) {
    ApiParam componentApiParam = null;
    for (Annotation[] parameterAnnotations : method.getParameterAnnotations()) {
      QueryParam queryParam = findAnnotation(parameterAnnotations, QueryParam.class);
      if (queryParam != null && "component".equals(queryParam.value())) {
        componentApiParam = findAnnotation(parameterAnnotations, ApiParam.class);
        break;
      }
    }

    assertNotNull(componentApiParam, "Missing @ApiParam for component on " + method.getName());
    assertTrue(componentApiParam.required(), "component must be marked required on " + method.getName());
  }

  private static <T extends Annotation> T findAnnotation(Annotation[] annotations, Class<T> annotationClass) {
    for (Annotation annotation : annotations) {
      if (annotationClass.isInstance(annotation)) {
        return annotationClass.cast(annotation);
      }
    }
    return null;
  }
}
