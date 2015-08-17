package com.linkedin.thirdeye.dashboard.resources;

import com.linkedin.thirdeye.dashboard.api.DimensionGroupSpec;
import com.linkedin.thirdeye.dashboard.util.ConfigCache;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.*;

@Path("/collection-config")
public class CollectionConfigResource {
  private static final String GROUPS_FILE_NAME = "groups.yml";
  private static final Logger LOG = LoggerFactory.getLogger(CollectionConfigResource.class);
  private final File collectionConfigRoot;
  private final ConfigCache configCache;

  public CollectionConfigResource(File collectionConfigRoot, ConfigCache configCache) {
    this.collectionConfigRoot = collectionConfigRoot;
    this.configCache = configCache;
  }

  @POST
  @Path("/{collection}/dimension-groups")
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  public Response createDimensionGroups(
      @PathParam("collection") String collection,
      byte[] dimensionGroupSpec) throws Exception {
    File collectionDir = new File(collectionConfigRoot, collection);
    File configFile = new File(collectionDir, GROUPS_FILE_NAME);
    writeConfigFile(configFile, dimensionGroupSpec);
    return Response.ok().build();
  }

  @GET
  @Path("/{collection}/dimension-groups")
  public Response getDimensionGroups(@PathParam("collection") String collection) throws Exception {
    File collectionDir = new File(collectionConfigRoot, collection);
    File configFile = new File(collectionDir, GROUPS_FILE_NAME);
    return readConfigFile(configFile);
  }

  @DELETE
  @Path("/{collection}/dimension-groups")
  public Response deleteDimensionGroups(@PathParam("collection") String collection) throws Exception {
    File collectionDir = new File(collectionConfigRoot, collection);
    File configFile = new File(collectionDir, GROUPS_FILE_NAME);
    configCache.invalidateDimensionGroupSpec(collection);
    return deleteConfigFile(configFile);
  }

  private Response readConfigFile(File configFile) throws Exception {
    // Paths should never be relative
    if (!configFile.isAbsolute()) {
      return Response.status(Response.Status.BAD_REQUEST).entity("Path is not absolute: " + configFile).build();
    }

    // Check if exists
    if (!configFile.exists()) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    try (InputStream inputStream = new FileInputStream(configFile)) {
      byte[] config = IOUtils.toByteArray(inputStream);
      return Response.ok(config, MediaType.APPLICATION_OCTET_STREAM).build();
    } catch (Exception e) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Could not read from " + configFile).build();
    }
  }

  private Response writeConfigFile(File configFile, byte[] config) throws Exception {
    // Paths should never be relative
    if (!configFile.isAbsolute()) {
      return Response.status(Response.Status.BAD_REQUEST).entity("Path is not absolute: " + configFile).build();
    }

    // Do not overwrite existing
    if (configFile.exists()) {
      return Response.status(Response.Status.CONFLICT).entity(GROUPS_FILE_NAME + " already exists").build();
    }

    // Create collection dir if it doesn't exist
    if (!configFile.getParentFile().exists()) {
      FileUtils.forceMkdir(configFile.getParentFile());
      LOG.info("Created {}", configFile.getParentFile());
    }

    // Write new config file
    try (OutputStream outputStream = new FileOutputStream(configFile)) {
      IOUtils.copy(new ByteArrayInputStream(config), outputStream);
      LOG.info("Created {}", configFile);
    } catch (Exception e) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Could not write to " + configFile).build();
    }

    return Response.ok().build();
  }

  private Response deleteConfigFile(File configFile) throws Exception {
    // Paths should never be relative
    if (!configFile.isAbsolute()) {
      return Response.status(Response.Status.BAD_REQUEST).entity("Path is not absolute: " + configFile).build();
    }

    // Check if exists
    if (!configFile.exists()) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    try {
      FileUtils.forceDelete(configFile);
      LOG.info("Deleted {}", configFile);
      return Response.noContent().build();
    } catch (Exception e) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Could not delete " + configFile).build();
    }
  }
}
