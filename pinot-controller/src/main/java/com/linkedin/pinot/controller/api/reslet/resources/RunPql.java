package com.linkedin.pinot.controller.api.reslet.resources;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;

import org.antlr.runtime.RecognitionException;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.ServerResource;

import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.pql.parsers.PQLCompiler;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Dec 8, 2014
 */

public class RunPql extends ServerResource {

  private static final Logger logger = Logger.getLogger(RunPql.class);
  private final ControllerConf conf;
  private final PinotHelixResourceManager manager;
  private static final PQLCompiler compiler = new PQLCompiler(new HashMap<String, String[]>());;

  public RunPql() {
    conf = (ControllerConf) getApplication().getContext().getAttributes().get(ControllerConf.class.toString());
    manager = (PinotHelixResourceManager) getApplication().getContext().getAttributes().get(PinotHelixResourceManager.class.toString());
  }

  @Override
  public Representation get() {
    final String pqlString = getQuery().getValues("pql");
    logger.info("*** found pql : " + pqlString);
    JSONObject compiledJSON;

    try {
      compiledJSON = compiler.compile(pqlString);
    } catch (final RecognitionException e) {
      logger.error(e);
      return new StringRepresentation(e.getMessage());
    }

    if (!compiledJSON.has("collection")) {
      return new StringRepresentation("your request does not contain the collection information");
    }

    String resource;

    try {
      final String collection = compiledJSON.getString("collection");
      resource = collection.substring(0, collection.indexOf("."));
    } catch (final JSONException e) {
      logger.error(e);
      return new StringRepresentation(e.getMessage());
    }

    String instanceId;
    try {
      instanceId = manager.getBrokerInstanceFor(resource);
    } catch (final Exception e) {
      logger.error(e);
      return new StringRepresentation(e.getMessage());
    }

    if (instanceId == null) {
      return new StringRepresentation("could not find a broker for data resource : " + resource);
    }

    final String host = instanceId.substring(0, instanceId.indexOf("_"));
    final String port = instanceId.substring(instanceId.indexOf("_") + 1, instanceId.length());
    final StringBuilder bld = new StringBuilder();
    bld.append(host);
    bld.append(":");
    bld.append(port);
    bld.append("/query?bql=");

    try {
      bld.append(URLEncoder.encode(pqlString, "UTF-8"));
    } catch (final UnsupportedEncodingException e) {
      return new StringRepresentation("trouble encoding pql");
    }

    URL url;
    try {
      url = new URL(bld.toString());
    } catch (final MalformedURLException e) {
      logger.error(e);

      return new StringRepresentation("error parsing url : " + e.getMessage());
    }
    BufferedReader reader = null;

    try {
      reader = new BufferedReader(new InputStreamReader(url.openStream(), "UTF-8"));
    } catch (final Exception e) {
      logger.error(e);
      return new StringRepresentation(e.getMessage());
    }

    final StringBuilder queryResp = new StringBuilder();
    try {
      for (String respLine; (respLine = reader.readLine()) != null;) {
        queryResp.append(respLine);
      }
    } catch (final IOException e) {
      logger.error(e);
      return new StringRepresentation(e.getMessage());
    }

    return new StringRepresentation(queryResp.toString());
  }
}
