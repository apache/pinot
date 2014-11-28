package com.linkedin.thirdeye.bootstrap.aggregation;

public enum MetricUnit {

  INT {
    public Number toNumber(String s) {
      return Integer.parseInt(s);
    }
  },
  SHORT {
    public Number toNumber(String s) {
      return Integer.parseInt(s);
    }
  },
  LONG {
    public Number toNumber(String s) {
      return Integer.parseInt(s);
    }
  },
  FLOAT {
    public Number toNumber(String s) {
      return Integer.parseInt(s);
    }
  },
  DOUBLE {
    public Number toNumber(String s) {
      return Integer.parseInt(s);
    }
  };

  public Number toNumber(String s) {
    throw new AbstractMethodError();
  }
}
