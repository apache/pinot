package com.linkedin.pinot.controller.api.restlet.resources;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.Restlet;
import org.restlet.engine.header.Header;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Finder;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.restlet.routing.Route;
import org.restlet.routing.Router;
import org.restlet.routing.TemplateRoute;
import org.restlet.util.RouteList;
import org.restlet.util.Series;

import com.linkedin.pinot.controller.api.ControllerRestApplication;
import com.linkedin.pinot.controller.api.swagger.Description;
import com.linkedin.pinot.controller.api.swagger.HttpVerb;
import com.linkedin.pinot.controller.api.swagger.Parameter;
import com.linkedin.pinot.controller.api.swagger.Paths;
import com.linkedin.pinot.controller.api.swagger.Summary;
import com.linkedin.pinot.controller.api.swagger.Tags;


/**
 * Resource that returns a Swagger definition of the API
 */
public class SwaggerResource extends ServerResource {
  @Get
  @Override
  public Representation get() {
    try {
      // Info
      JSONObject info = new JSONObject();
      info.put("title", "Pinot Controller");
      info.put("version", "0.1");

      // Paths
      JSONObject paths = new JSONObject();
      Router router = ControllerRestApplication.router;
      RouteList routeList = router.getRoutes();

      for (Route route : routeList) {
        if (route instanceof TemplateRoute) {
          TemplateRoute templateRoute = (TemplateRoute) route;
          JSONObject pathObject = new JSONObject();
          String routePath = templateRoute.getTemplate().getPattern();

          // Check which methods are present
          Restlet routeTarget = templateRoute.getNext();
          if (routeTarget instanceof Finder) {
            Finder finder = (Finder) routeTarget;
            Class<? extends ServerResource> targetClass = finder.getTargetClass();
            for (Method method : targetClass.getDeclaredMethods()) {
              String httpVerb = null;
              Annotation annotationInstance = method.getAnnotation(HttpVerb.class);
              if (annotationInstance != null) {
                httpVerb = ((HttpVerb) annotationInstance).value().toLowerCase();
              }

              HashSet<String> methodPaths = new HashSet<String>();
              annotationInstance = method.getAnnotation(Paths.class);
              if (annotationInstance != null) {
                methodPaths.addAll(Arrays.asList(((Paths) annotationInstance).value()));
              }

              if (httpVerb != null && methodPaths.contains(routePath) && !routePath.endsWith("/")) {
                JSONObject operation = new JSONObject();
                pathObject.put(httpVerb, operation);

                annotationInstance = method.getAnnotation(Summary.class);
                if (annotationInstance != null) {
                  operation.put(Summary.class.getSimpleName().toLowerCase(), ((Summary) annotationInstance).value());
                }

                annotationInstance = method.getAnnotation(Description.class);
                if (annotationInstance != null) {
                  operation.put(Description.class.getSimpleName().toLowerCase(), ((Description) annotationInstance).value());
                }

                annotationInstance = method.getAnnotation(Tags.class);
                if (annotationInstance != null) {
                  operation.put(Tags.class.getSimpleName().toLowerCase(), ((Tags) annotationInstance).value());
                }

                operation.put("operationId", method.getName());

                ArrayList<JSONObject> parameters = new ArrayList<JSONObject>();

                for (Annotation[] annotations : method.getParameterAnnotations()) {
                  if (annotations.length != 0) {
                    JSONObject parameter = new JSONObject();
                    for (Annotation annotation : annotations) {
                      if (annotation instanceof Parameter) {
                        Parameter parameterAnnotation = (Parameter) annotation;
                        parameter.put("name", parameterAnnotation.name());
                        parameter.put("in", parameterAnnotation.in());
                        if (parameterAnnotation.description() != null) {
                          parameter.put("description", parameterAnnotation.description());
                        }
                        parameter.put("type", "string");
                        parameter.put("required", parameterAnnotation.required());
                      }
                    }

                    if(parameter.keys().hasNext()) {
                      parameters.add(parameter);
                    }
                  }
                }

                operation.put("parameters", parameters.toArray(new JSONObject[parameters.size()]));
              }
            }
          }

          if (pathObject.keys().hasNext()) {
            paths.put(routePath, pathObject);
          }
        }
      }

      // Tags
      JSONArray tags = new JSONArray();
      addTag(tags, "tenant", "Tenant-related operations");
      addTag(tags, "instance", "Instance-related operations");
      addTag(tags, "table", "Table-related operations");
      addTag(tags, "segment", "Segment-related operations");
      addTag(tags, "schema", "Schema-related operations");

      // Swagger
      JSONObject swagger = new JSONObject();
      swagger.put("swagger", "2.0");
      swagger.put("info", info);
      swagger.put("paths", paths);
      swagger.put("tags", tags);

      StringRepresentation representation = new StringRepresentation(swagger.toString());

      // Set up CORS
      Series<Header> responseHeaders = (Series<Header>) getResponse().getAttributes().get("org.restlet.http.headers");
      if (responseHeaders == null) {
        responseHeaders = new Series(Header.class);
        getResponse().getAttributes().put("org.restlet.http.headers", responseHeaders);
      }
      responseHeaders.add(new Header("Access-Control-Allow-Origin", "*"));
      return representation;
    } catch (JSONException e) {
      return new StringRepresentation(e.toString());
    }
  }

  private void addTag(JSONArray tags, String tagName, String description) throws JSONException {
    JSONObject tag = new JSONObject();
    tag.put("name", tagName);
    tag.put("description", description);
    tags.put(tag);
  }
}
