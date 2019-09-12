package org.apache.pinot.benchmark.api;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.ws.rs.core.Response;
import org.apache.pinot.benchmark.PinotBenchConf;
import org.apache.pinot.benchmark.api.resources.PinotBenchException;
import org.apache.pinot.benchmark.common.utils.PinotClusterClient;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotClusterManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotClusterManager.class);

  private PinotBenchConf _conf;
  private FileUploadDownloadClient _fileUploadDownloadClient;
  private PinotClusterClient _pinotClusterClient;

  private String _perfHost;
  private int _perfPort;
  private String _prodHost;
  private int _prodPort;

  public PinotClusterManager(PinotBenchConf conf) {
    _conf = conf;
    _perfHost = conf.getPerfControllerHost();
    _perfPort = conf.getPerfControllerPort();
    _prodHost = conf.getProdControllerHost();
    _prodPort = conf.getProdControllerPort();
    _fileUploadDownloadClient = new FileUploadDownloadClient();
    _pinotClusterClient = new PinotClusterClient();
  }

  public List<String> listTables() {
    List<String> tableNames = new ArrayList<>();
    try {
      SimpleHttpResponse response =
          _pinotClusterClient.sendGetRequest(PinotClusterClient.getListAllTablesHttpURI(_perfHost, _perfPort));
      JsonNode jsonArray = JsonUtils.stringToJsonNode(response.getResponse()).get("tables");

      Iterator<JsonNode> elements = jsonArray.elements();
      while (elements.hasNext()) {
        tableNames.add(elements.next().asText());
      }
      return tableNames;
    } catch (Exception e) {
      e.printStackTrace();
      throw new PinotBenchException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR);
    }
  }
}
