package com.alexrnv.tripgen.workflow;

import com.alexrnv.tripgen.dto.DataObjects.Probe;
import com.alexrnv.tripgen.dto.DataObjects.Trip;
import com.alexrnv.tripgen.utils.GeoUtils;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.Random;

/**
 * Created: 2/4/16 2:30 PM
 * Author: alex
 */
public abstract class TripsWorkFlow {

    private static final long PROBES_TIME_DIF_THRESHOLD =  600 * 1000; //600s
    private static final double PROBES_DIST_DIF_THRESHOLD = 1000.; //1 km

    protected static final ThreadLocal<Trip.Builder> tripBuilder = ThreadLocal.withInitial(Trip::newBuilder);
    protected static final ThreadLocal<byte[]> bytes = ThreadLocal.withInitial(() -> new byte[4]);
    protected static final Random rnd = new Random();

    private final WorkFlowStats workFlowStats = new WorkFlowStats();

    public WorkFlowStats getWorkFlowStats() {
        return workFlowStats;
    }

    public void processProbe(Probe probe) throws IOException {

        long t0 = System.nanoTime(), tt0 = t0;
        Trip trip = findPendingTrip(probe.getSensorId());
        long t1 = System.nanoTime();
        workFlowStats.addFindTime(t1 - t0);

        if(trip == null) {
            workFlowStats.incCreated();
            createNewTrip(probe);
            workFlowStats.addCreateTime(System.nanoTime() - t1);
        } else {
            boolean b = tryUpdateTrip(trip, probe);
            t0 = System.nanoTime();
            workFlowStats.addUpdateTime(t0 - t1);
            if(!b) {
                workFlowStats.incCompleted();
                completePrevTripAndCreateNew(trip, probe);
                workFlowStats.addCompleteTime(System.nanoTime() - t0);
            } else {
                workFlowStats.incUpdated();
            }
        }
        workFlowStats.addAllTime(System.nanoTime() - tt0);
    }

    protected abstract void completePrevTripAndCreateNew(Trip prev, Probe probe) throws IOException;
    protected abstract void createNewTrip(Probe probe) throws IOException;
    protected abstract Trip findPendingTrip(ByteString sensorId) throws IOException;
    protected abstract void updateTrip(Trip updated) throws IOException;

    /**
     * Adds probe to current trip if it fits.
     * @return true if probe is added, false if it doesn't fit this trip.
     */
    protected boolean tryUpdateTrip(Trip trip, Probe probe) throws IOException {
        Trip updated = null;
        if(trip.getSensorId().equals(probe.getSensorId())) {

            long probeTime = probe.getTimestamp();
            long startTime = trip.getStartTime();
            long endTime = trip.getEndTime();
            int n = trip.getProbesCount();

            //duplicate start or end
            if(probeTime == startTime || probeTime == endTime) {
               return true;//no need to update in db
            }
            //probe in the middle of trip (rare case)
            else if (startTime < probeTime && endTime > probeTime) {
                for (int i = 0; i < n; i++) {
                    if (trip.getProbes(i).getTimestamp() > probeTime) {
                        updated = addProbeToTrip(trip, probe, i);
                        break;
                    }
                }
            }
            //first try to add in the end
            else if (endTime < probeTime && (probeTime - endTime) < PROBES_TIME_DIF_THRESHOLD) {
                Probe last = trip.getProbes(n - 1);
                if (GeoUtils.distanceM(last.getCellLat(), last.getCellLon(), probe.getCellLat(), probe.getCellLon()) < PROBES_DIST_DIF_THRESHOLD) {
                    updated = addProbeToTrip(trip, probe, n);
                }
            }
            //try to add in the beginning
            else if (startTime > probeTime && (startTime - probeTime) < PROBES_TIME_DIF_THRESHOLD) {
                Probe first = trip.getProbes(0);
                if (GeoUtils.distanceM(first.getCellLat(), first.getCellLon(), probe.getCellLat(), probe.getCellLon()) < PROBES_DIST_DIF_THRESHOLD) {
                    updated = addProbeToTrip(trip, probe, 0);
                }
            }
        }

        if(updated != null) {
            updateTrip(updated);
        }
        return updated != null;
    }

    protected Trip newTripFromProbe(Probe probe) {
        ByteString sensorId = probe.getSensorId();
        ByteString tripId = sensorId;
        Trip trip = tripBuilder.get()
                .clear()
                .setSensorId(sensorId)
                .setStartTime(probe.getTimestamp())
                .setEndTime(probe.getTimestamp())
                .setTripId(tripId)
                .addProbes(probe)
                .build();
        return trip;
    }

    protected Trip completeTrip(Trip trip) {
        rnd.nextBytes(bytes.get());
        return tripBuilder.get()
                .clear()
                .mergeFrom(trip)
                .setCompleted(true)
                .setTripId(trip.getTripId().concat(ByteString.copyFrom(bytes.get())))
                .build();
    }


    protected Trip addProbeToTrip(Trip trip, Probe probe, int pos) {
        return tripBuilder.get().clear().mergeFrom(trip).addProbes(pos, probe).build();
    }

}
