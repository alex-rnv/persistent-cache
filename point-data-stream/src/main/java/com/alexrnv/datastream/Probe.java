package com.alexrnv.datastream;

import org.apache.commons.lang3.StringUtils;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/**
 * Created: 3/8/16 12:07 AM
 * Author: alex
 */
public class Probe {

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
            .withZone(ZoneOffset.UTC);

    public double cellLat;
    public double cellLon;
    public ZonedDateTime timestamp;
    private String raw;
    //private char[] rawArray;
    public int updateCounter;

    public Probe(String input) {
        int i = input.indexOf("\t"),j;
        i = input.indexOf("\t", i + 1);
        j = input.indexOf("\t", i + 1);
        String w2 = input.substring(i+1,j);
        this.timestamp = ZonedDateTime.ofLocal(LocalDateTime.parse(w2, formatter), ZoneOffset.UTC, ZoneOffset.UTC);
        i = input.indexOf("\t", j+1);
        String w4 = input.substring(j+1,i);
        this.cellLat = Double.parseDouble(w4);
        j = input.indexOf("\t", i + 1);
        String w5 = input.substring(i+1,j);
        this.cellLon = Double.parseDouble(w5);
        this.raw = input;
        //this.rawArray = input.toCharArray();
    }

    public void update(double cellLatD, double cellLonD, int timeD) {
        this.cellLat += cellLatD;
        this.cellLon += cellLonD;
        this.timestamp = timestamp.plus(timeD, ChronoUnit.SECONDS);

        int i = raw.indexOf("\t"),j;
        i = raw.indexOf("\t", i + 1);
        j = raw.indexOf("\t", i + 1);
        String w2 = raw.substring(i+1,j);
        raw = StringUtils.replaceOnce(raw, w2, formatter.format(timestamp));
        i = raw.indexOf("\t", j + 1);
        String w4 = raw.substring(j+1,i);
        raw = StringUtils.replaceOnce(raw, w4, String.format("%12.8f", cellLat));
        j = raw.indexOf("\t", i + 1);
        String w5 = raw.substring(i+1,j);
        raw = StringUtils.replaceOnce(raw, w5, String.format("%12.8f", cellLon));
        updateCounter++;
    }

    @Override
    public String toString() {
        return raw;
    }

    public String toString(int n) {
        //rawArray[14]
        String x = String.format("%03d", n);
        String w = StringUtils.replace(raw, "000", x);
        return w;
    }
}
