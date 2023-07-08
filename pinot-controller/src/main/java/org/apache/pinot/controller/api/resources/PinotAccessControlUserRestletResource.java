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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.BcryptUtils;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.api.exception.UserAlreadyExistsException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.RBACAuthorization;
import org.apache.pinot.spi.config.user.ComponentType;
import org.apache.pinot.spi.config.user.UserConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = Constants.USER_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class PinotAccessControlUserRestletResource {
    /**
     * URI Mappings:
     * - "/user", "/users/": List all the users
     * - "/users/{username}", "/users/{username}/": List config for specified username.
     *
     * - "/user", "/users/" : Add a user
     * <pre>
     *       POST Request Body Example :
     *        {
     *         "username": "user1",
     *         "password": "user1@passwd",
     *         "component": "BROKER",
     *         "role" : "ADMIN",
     *         "tables": ["table1", "table2"],
     *         "permissions": ["READ"]
     *        }
     *  </pre>
     *
     *  - "/users/{username}", "/users/{username}/"
     *  PUT Request body example : same as POST Request Body
     * {@inheritDoc}
     */
    public static final Logger LOGGER = LoggerFactory.getLogger(PinotAccessControlUserRestletResource.class);

    @Inject
    PinotHelixResourceManager _pinotHelixResourceManager;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/users")
    @RBACAuthorization(targetType = "cluster", permission = "ListUsers")
    @ApiOperation(value = "List all uses in cluster", notes = "List all users in cluster")
    public String listUers() {
        try {
            ZkHelixPropertyStore<ZNRecord> propertyStore = _pinotHelixResourceManager.getPropertyStore();
            Map<String, UserConfig> allUserInfo = ZKMetadataProvider.getAllUserInfo(propertyStore);
            return JsonUtils.newObjectNode().set("users", JsonUtils.objectToJsonNode(allUserInfo)).toString();
        } catch (Exception e) {
            throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST, e);
        }
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/users/{username}")
    @RBACAuthorization(targetType = "cluster", permission = "ListUser")
    @ApiOperation(value = "Get an user in cluster", notes = "Get an user in cluster")
    public String getUser(@PathParam("username") String username, @QueryParam("component") String componentTypeStr) {
        try {
            ZkHelixPropertyStore<ZNRecord> propertyStore = _pinotHelixResourceManager.getPropertyStore();
            ComponentType componentType = Constants.validateComponentType(componentTypeStr);
            String usernameWithType = username + "_" + componentType.name();

            UserConfig userConfig = ZKMetadataProvider.getUserConfig(propertyStore, usernameWithType);
            return JsonUtils.newObjectNode().set(usernameWithType, JsonUtils.objectToJsonNode(userConfig)).toString();
        } catch (Exception e) {
            throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST, e);
        }
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/users")
    @RBACAuthorization(targetType = "cluster", permission = "AddUser")
    @ApiOperation(value = "Add a user", notes = "Add a user")
    public SuccessResponse addUser(String userConfigStr) {
        // TODO introduce a table config ctor with json string.

        UserConfig userConfig;
        String username;
        try {
            userConfig = JsonUtils.stringToObject(userConfigStr, UserConfig.class);
            username = userConfig.getUserName();
            if (username.contains(".") || username.contains(" ")) {
                throw new IllegalStateException("Username: " + username + " containing '.' or space is not allowed");
            }
        } catch (Exception e) {
            throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST, e);
        }
        try {
            _pinotHelixResourceManager.addUser(userConfig);
            return new SuccessResponse(String.format("User %s has been successfully added!",
                userConfig.getUserName() + '_' + userConfig.getComponentType()));
        } catch (Exception e) {
            if (e instanceof UserAlreadyExistsException) {
                throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT, e);
            } else {
                throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST, e);
            }
        }
    }

    @DELETE
    @Path("/users/{username}")
    @RBACAuthorization(targetType = "cluster", permission = "DeleteUser")
    @Authenticate(AccessType.DELETE)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Delete a user", notes = "Delete a user")
    public SuccessResponse deleteUser(@PathParam("username") String username,
        @QueryParam("component") String componentTypeStr) {

        List<String> usersDeleted = new LinkedList<>();
        String usernameWithComponentType = username + "_" + componentTypeStr;

        try {

            boolean userExist = false;
            userExist = _pinotHelixResourceManager.hasUser(username, componentTypeStr);

            _pinotHelixResourceManager.deleteUser(usernameWithComponentType);
            if (userExist) {
                usersDeleted.add(username);
            }
            if (!usersDeleted.isEmpty()) {
                return new SuccessResponse("User: " + usernameWithComponentType + " has been successfully deleted");
            }
        } catch (Exception e) {
            throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST, e);
        }

        throw new ControllerApplicationException(LOGGER,
            "User " + usernameWithComponentType + " does not exists", Response.Status.NOT_FOUND);
    }


    @PUT
    @Path("/users/{username}")
    @RBACAuthorization(targetType = "cluster", permission = "UpdateUser")
    @Authenticate(AccessType.UPDATE)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update user config for a user", notes = "Update user config for user")
    public SuccessResponse updateUserConfig(
        @PathParam("username") String username,
        @QueryParam("component") String componentTypeStr,
        @QueryParam("passwordChanged") boolean passwordChanged,
        String userConfigString) {

        UserConfig userConfig;
        String usernameWithComponentType = username + "_" + componentTypeStr;
        try {
            userConfig = JsonUtils.stringToObject(userConfigString, UserConfig.class);
            if (passwordChanged) {
                userConfig.setPassword(BcryptUtils.encrypt(userConfig.getPassword()));
            }
            String usernameWithComponentTypeFromUserConfig = userConfig.getUsernameWithComponent();
            if (!usernameWithComponentType.equals(usernameWithComponentTypeFromUserConfig)) {
                throw new ControllerApplicationException(LOGGER,
                    String.format("Request user %s does not match %s in the Request body",
                        usernameWithComponentType, usernameWithComponentTypeFromUserConfig),
                    Response.Status.BAD_REQUEST);
            }
            if (!_pinotHelixResourceManager.hasUser(username, componentTypeStr)) {
                throw new ControllerApplicationException(LOGGER,
                    "Request user " + usernameWithComponentType + " does not exist",
                    Response.Status.NOT_FOUND);
            }
            _pinotHelixResourceManager.updateUserConfig(userConfig);
        } catch (Exception e) {
            throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST, e);
        }
        return new SuccessResponse("User config update for " + usernameWithComponentType);
    }
}
