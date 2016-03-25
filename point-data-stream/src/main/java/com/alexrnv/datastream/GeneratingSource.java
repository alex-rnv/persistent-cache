package com.alexrnv.datastream;

import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created: 3/8/16 12:48 AM
 * Author: alex
 */
@Component
public class GeneratingSource {

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
            .withZone(ZoneOffset.UTC);

    private int TRIPS_DAILY = 1_000_000;
    private int TRIPS_MULTIPLIER = 300;
    private int LOG_LIM = 100_000;

    private final int STOP_TIME_SEC = 600;
    private final int MIN_TRIP_LEN = 3;
    private final int MAX_TRIP_LEN = 15;
    private final ZonedDateTime startDate = ZonedDateTime.parse("2015-04-15T00:41:04+00:00");
    private final String TEMPLATE_STR = "%s\t%s\t%s\t%12.8f\t%12.8f\t57\tkph\t166\t%s\t\\N";

    private final Random rnd = new Random();

    @Value("${source.folder}")
    private String folder;

    private List<Probe> currentProbes = new ArrayList<>(TRIPS_DAILY);

    @PostConstruct
    private void init() {
        for(int trip=0; trip< TRIPS_DAILY; trip++) {
            String date = startDate.plus(rnd.nextInt(6)-3, ChronoUnit.SECONDS).format(formatter);
            double startLat = rnd.nextInt(700)/10.1 + 10.;
            double startLon = rnd.nextInt(1600)/10.1 + 10.;
            String raw = String.format(TEMPLATE_STR,
                    RandomStringUtils.randomAlphanumeric(12) + "000",
                    date, date,
                    startLat, startLon,
                    RandomStringUtils.randomAlphanumeric(9) + "000");
            Probe probe = new Probe(raw);
            currentProbes.add(probe);
            if(trip % LOG_LIM == 0) System.out.printf("Initialization: %d\n", trip);
        }
        System.out.println("Initialization complete");

    }

    public void produce(EventStream stream) {
        int s=0;
        for(Probe p : currentProbes) {
            stream.add(p.toString());
            for(int m=1; m<TRIPS_MULTIPLIER; m++) {
                stream.add(p.toString(m));
            }
            if(s % LOG_LIM == 0) System.out.printf("Initial probes sent: %d\n", (s++)*TRIPS_MULTIPLIER);
        }
        s=0;
        while(true) {
            for (Probe p : currentProbes) {
                int r;
                boolean nextTrip = p.updateCounter > MIN_TRIP_LEN;
                if (nextTrip) {
                    int k = MAX_TRIP_LEN - p.updateCounter;
                    if (k <= 0) {
                        nextTrip = true;
                    } else {
                        r = rnd.nextInt(k);
                        nextTrip = r < 1;
                    }
                }
                int timeD = STOP_TIME_SEC / 2;
                if (nextTrip) {
                    p.updateCounter = 0;
                    timeD = STOP_TIME_SEC * 2;
                }

                p.update(0.0021455, 0.0024342, timeD);
                stream.add(p.toString());
                for(int m=1; m<TRIPS_MULTIPLIER; m++) {
                    stream.add(p.toString(m));
                }
                if(s % LOG_LIM == 0) System.out.printf("Probes sent: %d\n", (s++)*TRIPS_MULTIPLIER);
            }
        }
    }
}
