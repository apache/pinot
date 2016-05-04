package com.linkedin.thirdeye.api;

/**
 * Represents the various data types supported for a metric<br/>
 * Currently we support INT, SHORT, LONG, FLOAT, DOUBLE
 * @author kgopalak
 */
public enum MetricType {

  INT {
    public Number toNumber(String s) {
      return Integer.parseInt(s);
    }

    public int byteSize() {
      return 4;
    }

  },
  SHORT {
    public Number toNumber(String s) {
      return Short.parseShort(s);
    }

    public int byteSize() {
      return 2;

    }

  },
  LONG {
    public Number toNumber(String s) {
      return Long.parseLong(s);
    }

    public int byteSize() {
      return 8;

    }

  },
  FLOAT {
    public Number toNumber(String s) {
      return Float.parseFloat(s);
    }

    public int byteSize() {
      return 4;

    }

  },
  DOUBLE {
    public Number toNumber(String s) {
      return Double.parseDouble(s);
    }

    public int byteSize() {
      return 8;
    }
  };

  public Number toNumber(String s) {
    throw new AbstractMethodError();
  }

  public int byteSize() {
    throw new AbstractMethodError();
  }
}
