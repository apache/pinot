package org.apache.pinot.core.query.request.context.predicate;

import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.locationtech.jts.geom.Geometry;


//TODO: Make this flexible
public class GeoPredicate {

  //this is the column name
  ExpressionContext _lhs;

  Predicate type;

  Geometry _geometry;

  double _distance;

  public enum Pre {
    WITHIN, OVERLAP;
  }

  public ExpressionContext getLhs() {
    return _lhs;
  }

  public void setLhs(ExpressionContext lhs) {
    _lhs = lhs;
  }

  public Geometry getGeometry() {
    return _geometry;
  }

  public void setGeometry(Geometry geometry) {
    _geometry = geometry;
  }

  public double getDistance() {
    return _distance;
  }

  public void setDistance(double distance) {
    _distance = distance;
  }
}
