package com.linkedin.thirdeye.realtime;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

// TODO: Maybe this should be in a proper class, but just hacks for now
public class ThirdEyeIPUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeIPUtils.class);
  private static final String TAR_NAME = "dbip-country-2015-05.csv.tgz";
  private static final SortedMap<IPV4Address, IPV4Range> START_ADDRESSES = new TreeMap<>();
  static {
    try {
      InputStream gzipStream = new GZIPInputStream(ClassLoader.getSystemResourceAsStream(TAR_NAME));
      TarArchiveInputStream tar = new TarArchiveInputStream(gzipStream);
      TarArchiveEntry entry = null;

      while ((entry = tar.getNextTarEntry()) != null) {
        byte[] data = new byte[(int) entry.getSize()];
        tar.read(data);
        InputStream inputStream = new ByteArrayInputStream(data);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line = null;
        while ((line = reader.readLine()) != null) {
          String[] tokens = line.split(","); // [start, end, country]
          IPV4Address start = new IPV4Address(tokens[0].replaceAll("\"", ""));
          IPV4Address end = new IPV4Address(tokens[1].replaceAll("\"", ""));
          String country = tokens[2].replaceAll("\"", "");
          IPV4Range range = new IPV4Range(start, end, country);
          START_ADDRESSES.put(start, range);
        }
      }
    } catch (Exception e) {
      LOG.error("Could not load geo IP data", e);
    }
  }

  public static String getCountry(String ipAddressString) {
    // TODO: I think there is a bug when address is the start of range
    IPV4Address address = new IPV4Address(ipAddressString);
    SortedMap<IPV4Address, IPV4Range> headMap = START_ADDRESSES.headMap(address);
    IPV4Range range = START_ADDRESSES.get(headMap.lastKey());
    if (range != null && range.contains(address)) {
      return range.getCountry();
    }
    return "UNKNOWN";
  }

  private static class IPV4Range implements Comparable<IPV4Range> {
    private final IPV4Address start;
    private final IPV4Address end;
    private final String country;

    IPV4Range(IPV4Address start, IPV4Address end, String country) {
      this.start = start;
      this.end = end;
      this.country = country;
    }

    IPV4Address getStart() {
      return start;
    }

    IPV4Address getEnd() {
      return end;
    }

    String getCountry() {
      return country;
    }

    boolean contains(IPV4Address address) {
      return address.compareTo(start) >= 0 && address.compareTo(end) <= 0;
    }

    @Override
    public int compareTo(IPV4Range range) {
      return start.compareTo(range.getStart());
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof IPV4Range)) {
        return false;
      }
      IPV4Range range = (IPV4Range) o;
      return start.equals(range.getStart()) && end.equals(range.getEnd());
    }
  }

  private static class IPV4Address implements Comparable<IPV4Address> {
    private final int[] components;

    IPV4Address(String address) {
      String[] tokens = address.split("\\.");
      if (tokens.length != 4) {
        throw new IllegalArgumentException("Invalid IP address " + address);
      }
      components = new int[tokens.length];
      for (int i = 0; i < tokens.length; i++) {
        components[i] = Integer.valueOf(tokens[i]);
      }
    }

    int[] getComponents() {
      return components;
    }

    @Override
    public int compareTo(IPV4Address address) {
      for (int i = 0; i < components.length; i++) {
        if (components[i] != address.getComponents()[i]) {
          return components[i] - address.getComponents()[i];
        }
      }
      return 0;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof IPV4Address)) {
        return false;
      }
      IPV4Address address = (IPV4Address) o;
      return Arrays.equals(components, address.getComponents());
    }
  }
}
