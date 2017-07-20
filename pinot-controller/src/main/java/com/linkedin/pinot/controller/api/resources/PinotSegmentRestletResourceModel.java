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

package com.linkedin.pinot.controller.api.resources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.utils.CommonConstants;
import io.swagger.annotations.Api;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Current URI Mappings:
 * <ul>
 *   <li>
 *     "/tables/{tableName}/segments/{segmentName}":
 *     "/tables/{tableName}/segments/{segmentName}/metadata":
 *     Get segment metadata for a given segment
 *   </li>
 *   <li>
 *     "/tables/{tableName}/segments":
 *     "/tables/{tableName}/segments/metadata":
 *     List segment metadata for a given table
 *   </li>
 *   <li>
 *      "/tables/{tableName}/segments/crc":
 *      Get crc information for a given table
 *   </li>
 *   <li>
 *     "/tables/{tableName}/segments/{segmentName}?state={state}":
 *     Change the state of the segment to specified {state} (enable|disable|drop)
 *   </li>
 *   <li>
 *     "/tables/{tableName}/segments?state={state}":
 *     Change the state of all segments of the table to specified {state} (enable|disable|drop)
 *   </li>
 *   <li>
 *     "/tables/{tableName}/segments/{segmentName}/reload":
 *     Reload the segment
 *   </li>
 *   <li>
 *     "/tables/{tableName}/segments/reload":
 *     Reload all segments of the table
 *   </li>
 * </ul>
 *
 * {@inheritDoc}
 * @see org.restlet.resource.ServerResource#get()
 */

@Api(tags = "sometag")
@Path("/")
public class PinotSegmentRestletResourceModel {
  public static Logger LOGGER = LoggerFactory.getLogger(PinotSegmentRestletResourceModel.class);
  public static final Response.Status BAD_REQUEST = Response.Status.BAD_REQUEST;
  public static final Response.Status INTERNAL_ERROR = Response.Status.INTERNAL_SERVER_ERROR;


  enum State {
    ENABLE,
    DISABLE,
    DROP,
  }

  /* THIS ONE WORKS, but we need to define the @Api in an ambiguous way.
  @GET
  @Path("table/{tableName}/segments")
  @Produces(MediaType.APPLICATION_JSON)
  public String toggleStateOrListMetadata(
    @PathParam("tableName") String tableName,
    @QueryParam("state") String stateStr,
    @QueryParam("type") String tableTypeStr
  ) {
    return toggleStateOrListMetadataInternal(tableName, stateStr, tableTypeStr);
  }
  */

  /** THIS DOES NOT WORK, but we can define @Api in unambiguous way.
  @GET
  @Path("table/{tableName}/segments")
  @Produces(MediaType.APPLICATION_JSON)
  public String listMetadata(
      @PathParam("tableName") String tableName,
      @QueryParam("type") String tableTypeStr
  ) {
    return toggleStateOrListMetadataInternal(tableName, null, tableTypeStr);
  }

  @GET
  @Path("table/{tableName}/segments")
  @Produces(MediaType.APPLICATION_JSON)
  public String toggleState(
      @PathParam("tableName") String tableName,
      @QueryParam("state") String stateStr,
      @QueryParam("type") String tableTypeStr
  ) {
    return toggleStateOrListMetadataInternal(tableName, stateStr, tableTypeStr);
  }
   */

  @GET
  @Path("table/{tableName}/segments/metadata")
  @Produces(MediaType.APPLICATION_JSON)
  public String listMetadataForAllSegments(
      @PathParam("tableName") String tableName,
      @QueryParam("state") String stateStr,
      @QueryParam("type") String tableTypeStr
  ) {
    return toggleStateOrListMetadataInternal(tableName, null, tableTypeStr);
  }

  @GET
  @Path("table/{tableName}/segments/{segmentName}/metadata")
  @Produces(MediaType.APPLICATION_JSON)
  public String listMetadataForOneSegment(
      @PathParam("tableName") String tableName,
      @PathParam("segmentName") String segmentName,
      @QueryParam("type") String tableTypeStr
  ) {
    if (tableName == null) {
      throw new WebApplicationException("Table type cannot be null", BAD_REQUEST);
    }
    CommonConstants.Helix.TableType tableType = validateTableType(tableTypeStr);
    if (tableType != null) {
      return "List metadata for segment " + segmentName + " in table type " + tableTypeStr;
    }
    return "List metadata for segment " + segmentName + " in any table type";
  }

  @GET
  @Path("table/{tableName}/segments/{segmentName}/reload")
  @Produces(MediaType.APPLICATION_JSON)
  public String reloadOneSegment(
      @PathParam("tableName") String tableName,
      @PathParam("segmentName") String segmentName,
      @QueryParam("type") String tableTypeStr
  ) {
    if (tableName == null) {
      throw new WebApplicationException("Table type cannot be null", BAD_REQUEST);
    }
    CommonConstants.Helix.TableType tableType = validateTableType(tableTypeStr);
    if (tableType != null) {
      return "Reload segment " + segmentName + " in table type " + tableTypeStr;
    }
    return "Reload segment " + segmentName + " in any table type";
  }

  @GET
  @Path("table/{tableName}/segments/reload")
  @Produces(MediaType.APPLICATION_JSON)
  public String reloadAllSegments(
      @PathParam("tableName") String tableName,
      @QueryParam("type") String tableTypeStr
  ) {
    if (tableName == null) {
      throw new WebApplicationException("Table type cannot be null", BAD_REQUEST);
    }
    CommonConstants.Helix.TableType tableType = validateTableType(tableTypeStr);
    if (tableType != null) {
      return "Reload all segments for table type " + tableTypeStr;
    }
    return "Reload all segments";
  }

  @GET
  @Path("table/{tableName}/segments/crc")
  @Produces(MediaType.APPLICATION_JSON)
  public String getCrcForAllSegments(
      @PathParam("tableName") String tableName,
      @QueryParam("type") String tableTypeStr
  ) {
    if (tableName == null) {
      throw new WebApplicationException("Table type cannot be null", BAD_REQUEST);
    }
    CommonConstants.Helix.TableType tableType = validateTableType(tableTypeStr);
    if (tableType != null) {
      return "get crc all segments for table type " + tableTypeStr;
    }
    return "get crc for all segments";
  }

  private CommonConstants.Helix.TableType validateTableType(String tableTypeStr) {
    if (tableTypeStr == null) {
      return null;
    }
    try {
      return CommonConstants.Helix.TableType.valueOf(tableTypeStr.toUpperCase());
    } catch (IllegalArgumentException e) {
      LOGGER.info("Illegal table type '{}'", tableTypeStr);
      throw new WebApplicationException("Illegal table type '" + tableTypeStr + "'", BAD_REQUEST);
    }
  }

  private State validateState(String stateStr) {
    if (stateStr == null) {
      return null;
    }
    try {
      return State.valueOf(stateStr.toUpperCase());
    } catch (IllegalArgumentException e) {
      LOGGER.info("Illegal state '{}'", stateStr);
      throw new WebApplicationException("Illegal state '" + stateStr + "'", BAD_REQUEST);
    }
  }

  private String toggleStateOrListMetadataInternal(String tableName, String stateStr, String tableTypeStr) {
    if (tableName == null) {
      throw new WebApplicationException("Table type cannot be null", BAD_REQUEST);
    }
    CommonConstants.Helix.TableType tableType = validateTableType(tableTypeStr);
    State state = validateState(stateStr);

    String rv = "FAILED";

    if (state == null) {
      if (tableType == null) {
        rv = "List metadata for all segments of table " + tableName;
      } else {
        if (tableType.equals(CommonConstants.Helix.TableType.OFFLINE)) {
          rv = "List metadata for offline segments of table " + tableName;
        } else {
          rv = "List metadata for realtime segments of table " + tableName;
        }
      }
    } else {
      switch (state) {
        case DISABLE:
          if (tableType == null) {
            rv = "set offline and realtime segment states to be DISABLED for table " + tableName;
          } else  if (tableType.equals(CommonConstants.Helix.TableType.OFFLINE)) {
            rv = "set offline segment states to be DISABLED for table " + tableName;
          } else {
            rv = "set realtime segment states to DISABLED for table " + tableName;
          }
          break;
        case ENABLE:
          if (tableType == null) {
            rv = "set offline and realtime segment states to be ENABLED for table " + tableName;
          } else if (tableType.equals(CommonConstants.Helix.TableType.OFFLINE)) {
            rv = "set offline segment states to be ENABLED for table " + tableName;
          } else {
            rv = "set realtime segment states to ENABLED for table " + tableName;
          }
          break;
        case DROP:
          if (tableType == null) {
            rv = "set offline and realtime segment states to be DROPPED for table " + tableName;
          } else if (tableType.equals(CommonConstants.Helix.TableType.OFFLINE)) {
            rv = "set offline segment states to be DROPPED for table " + tableName;
          } else {
            rv = "set realtime segment states to DROPPED for table " + tableName;
          }
          break;
      }
    }

    return rv;
  }
}
