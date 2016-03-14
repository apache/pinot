package com.linkedin.pinot.common.restlet;

import com.linkedin.pinot.common.restlet.swagger.Paths;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;
import java.util.TreeSet;
import org.apache.commons.collections.ComparatorUtils;
import org.restlet.Application;
import org.restlet.Restlet;
import org.restlet.resource.ServerResource;
import org.restlet.routing.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base class for Pinot restlet applications.
 */
public abstract class PinotRestletApplication extends Application {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotRestletApplication.class);
  private static Router router;

  @Override
  public Restlet createInboundRoot() {
    router = new Router(getContext());
    configureRouter(router);
    return router;
  }

  protected abstract void configureRouter(Router router);

  protected void attachRoutesForClass(Router router, Class<? extends ServerResource> clazz) {
    TreeSet<String> pathsOrderedByLength = new TreeSet<String>(ComparatorUtils.chainedComparator(new Comparator<String>() {

      @Override
      public int compare(String left, String right) {
        int leftLength = left.length();
        int rightLength = right.length();
        return leftLength < rightLength ? -1 : (leftLength == rightLength ? 0 : 1);
      }
    }, ComparatorUtils.NATURAL_COMPARATOR));

    for (Method method : clazz.getDeclaredMethods()) {
      Annotation annotationInstance = method.getAnnotation(Paths.class);
      if (annotationInstance != null) {
        pathsOrderedByLength.addAll(Arrays.asList(((Paths) annotationInstance).value()));
      }
    }

    for (String routePath : pathsOrderedByLength) {
      LOGGER.info("Attaching route {} -> {}", routePath, clazz.getSimpleName());
      attachRoute(router, routePath, clazz);
    }
  }

  protected void attachRoute(Router router, String routePath, Class<? extends ServerResource> clazz) {
    router.attach(routePath, createFinder(clazz));
  }

  public static Router getRouter() {
    return router;
  }
}
