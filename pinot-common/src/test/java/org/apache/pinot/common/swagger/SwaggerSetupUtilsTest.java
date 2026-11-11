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
package org.apache.pinot.common.swagger;

import io.swagger.jaxrs.config.BeanConfig;
import io.swagger.models.SecurityRequirement;
import io.swagger.models.Swagger;
import io.swagger.models.auth.ApiKeyAuthDefinition;
import io.swagger.models.auth.In;
import io.swagger.models.auth.SecuritySchemeDefinition;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SwaggerSetupUtilsTest {
  @Test
  public void testBuildSwaggerConfigAddsAuthorizationHeaderSecurityDefinition() {
    BeanConfig beanConfig =
        SwaggerSetupUtils.buildSwaggerConfig("Controller", "org.apache.pinot.common.swagger", false, "/");
    beanConfig.setScan(true);
    Swagger swagger = beanConfig.getSwagger();

    Map<String, SecuritySchemeDefinition> securityDefinitions = swagger.getSecurityDefinitions();
    Assert.assertNotNull(securityDefinitions);
    Assert.assertFalse(securityDefinitions.isEmpty());
    SecuritySchemeDefinition securityDefinition = securityDefinitions.get(CommonConstants.SWAGGER_AUTHORIZATION_KEY);
    Assert.assertTrue(securityDefinition instanceof ApiKeyAuthDefinition);

    ApiKeyAuthDefinition apiKeyAuthDefinition = (ApiKeyAuthDefinition) securityDefinition;
    Assert.assertEquals(apiKeyAuthDefinition.getName(), HttpHeaders.AUTHORIZATION);
    Assert.assertEquals(apiKeyAuthDefinition.getIn(), In.HEADER);
    Assert.assertEquals(apiKeyAuthDefinition.getType(), "apiKey");

    List<SecurityRequirement> securityRequirements = swagger.getSecurity();
    Assert.assertNotNull(securityRequirements);
    Assert.assertEquals(securityRequirements.size(), 1);
    Map<String, List<String>> requirements = securityRequirements.get(0).getRequirements();
    Assert.assertTrue(requirements.containsKey(CommonConstants.SWAGGER_AUTHORIZATION_KEY));
    Assert.assertTrue(requirements.get(CommonConstants.SWAGGER_AUTHORIZATION_KEY).isEmpty());
  }
}
