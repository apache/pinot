package org.apache.pinot.common.utils;

import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.DualHashBidiMap;

import java.time.ZoneId;
import java.util.Set;

public class PinotTimeZoneIdMapper {

    static final private BidiMap<String, Integer> zoneIdByPinotTZId;

    private static final long TIMEZONE_MASK = 0xFFF;

    static {
        Set<String> zoneIds = ZoneId.getAvailableZoneIds();
        zoneIdByPinotTZId = new DualHashBidiMap<>();
        int id = 0;
        for (String zoneId : zoneIds) {
            zoneIdByPinotTZId.put(zoneId, id++);
        }
    }

    public static int pinotTZId(String zoneId) {
        Integer id = zoneIdByPinotTZId.get(zoneId);
        if (id == null) {
            throw new IllegalArgumentException("Unknown Zone ID: " + zoneId);
        }

        return id;
    }

    public static String zoneId(Integer pinotTZId) {
        return zoneIdByPinotTZId.getKey(pinotTZId);
    }

    public static long packInstantAndTimeZone(long instant, int pinotTimeZoneId) {
        if (pinotTimeZoneId < 0 || pinotTimeZoneId >= 4096) {
            throw new IllegalArgumentException("pinotTimeZoneId must be between 0 and 4095 (12 bits).");
        }

        // move the instant to the upper 52 bits and combine with the pinotTimeZoneId in the lower 12 bits
        return (instant & 0xFFFFFFFFFFFFF000L) | (pinotTimeZoneId & TIMEZONE_MASK);
    }

    public static long unpackInstant(long dateTimeWithTimeZone) {
        return (dateTimeWithTimeZone >> 12) & 0xFFFFFFFFFFFFFL;
    }

    public static int unpackTimeZoneId(long dateTimeWithTimeZone) {
        return (int) (dateTimeWithTimeZone & TIMEZONE_MASK); // Extract the lower 12 bits (pinotTimeZoneId)
    }
}
