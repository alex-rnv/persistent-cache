package com.alexrnv.tripgen.convert;

import com.alexrnv.tripgen.dto.DataObjects;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.ArrayUtils;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

/**
 * Created: 2/10/16 6:42 PM
 * Author: alex
 */
public class TsvProbeConverter implements ProbeConverter<String> {

    private static final ThreadLocal<DataObjects.Probe.Builder> probeBuilder = ThreadLocal.withInitial(() -> DataObjects.Probe.newBuilder());
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);
    private static final Random rnd = new Random();

    @Override
    public DataObjects.Probe convert(String input) {
//        String[] words = input.split("\t");
//        return probeBuilder.get()
//                .clear()
//                .setCellId(words[0])
//                .setTimestamp(ZonedDateTime.parse(words[2], formatter).toInstant().toEpochMilli())
//                .setSensorId(StringUtils.reverse(words[0]))
//                .setEventType(DataObjects.EventType.LOCATION_UPDATE)
//                .setCellLat(Double.parseDouble(words[3]))
//                .setCellLon(Double.parseDouble(words[4]))
//                .setId(rnd.nextLong())
//                .build();


        //some reduce gc crap
        DataObjects.Probe.Builder builder = probeBuilder.get().clear();
        int i = input.indexOf("\t"),j;
        String w0 = input.substring(0, i);
        byte[] bytes = w0.getBytes();
        builder.setCellId(ByteString.copyFrom(bytes));
        ArrayUtils.reverse(bytes);
        builder.setSensorId(ByteString.copyFrom(bytes));
        i = input.indexOf("\t", i+1);
        j = input.indexOf("\t", i+1);
        String w2 = input.substring(i+1,j);
        builder.setTimestamp(ZonedDateTime.parse(w2, formatter).toInstant().toEpochMilli());
        i = input.indexOf("\t", j+1);
        String w4 = input.substring(j+1,i);
        builder.setCellLat(Double.parseDouble(w4));
        j = input.indexOf("\t", i+1);
        String w5 = input.substring(i+1,j);
        builder.setCellLon(Double.parseDouble(w5));
        return builder
                .setEventType(DataObjects.EventType.LOCATION_UPDATE)
                .setId(rnd.nextLong())
                .build();
    }
}
