package org.apache.pinot.common.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedMap;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DatabaseUtils {
  private DatabaseUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseUtils.class);

  private static final List<String> TABLE_NAME_KEYS = List.of("tableName", "tableNameWithType", "schemaName");

  public static void translateTableNameQueryParam(ContainerRequestContext requestContext, TableCache tableCache) {
    MultivaluedMap<String, String> queryParams = requestContext.getUriInfo().getQueryParameters();
    String uri = requestContext.getUriInfo().getRequestUri().toString();
    String databaseName = null;
    if (requestContext.getHeaders().containsKey(CommonConstants.DATABASE)) {
      databaseName = requestContext.getHeaderString(CommonConstants.DATABASE);
    }
    for (String key : TABLE_NAME_KEYS) {
      if (queryParams.containsKey(key)) {
        String tableName = queryParams.getFirst(key);
        String actualTableName = translateTableName(tableName, databaseName, tableCache);
        // table is not part of default database
        if (!actualTableName.equals(tableName)) {
          uri = uri.replaceAll(String.format("%s=%s", key, tableName),
              String.format("%s=%s", key, actualTableName));
          try {
            requestContext.setRequestUri(new URI(uri));
          } catch (URISyntaxException e) {
            LOGGER.error("Unable to translate the table name from {} to {}", tableName, actualTableName);
          }
        }
      }
    }
  }

  public static String translateTableName(String tableName, String databaseName, TableCache tableCache) {
    String[] tableSplit = tableName.split("\\.");
    if (tableSplit.length > 2) {
      throw new IllegalStateException("Table name: '" + tableName + "' containing more than one '.' is not allowed");
    } else if (tableSplit.length == 2) {
      databaseName = tableSplit[0];
      tableName = tableSplit[1];
    }
    if (databaseName != null && !databaseName.isBlank()) {
      tableName = String.format("%s.%s", databaseName, tableName);
    }
    String actualTableName = null;
    if (tableCache != null) {
      actualTableName = tableCache.getActualTableName(tableName);
    }
    return actualTableName != null ? actualTableName : tableName;
  }
}
