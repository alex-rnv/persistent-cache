package com.alexrnv.tripgen.workflow;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.alexrnv.tripgen.dto.DataObjects.Probe;
import com.alexrnv.tripgen.dto.DataObjects.Trip;
import com.google.protobuf.ByteString;

import java.io.IOException;

/**
 * Created: 2/24/16 9:10 AM
 * Author: alex
 */
public class AerospikeTripsWorkflowImpl extends TripsWorkFlow {

    private AerospikeClient client;
    private final String NAMESPACE = "test";

    public AerospikeTripsWorkflowImpl() {
        client = new AerospikeClient("127.0.0.1", 3000);
    }

    @Override
    protected void completePrevTripAndCreateNew(Trip prev, Probe probe) throws IOException {
        Trip completed = completeTrip(prev);
        updateTrip(completed);
        createNewTrip(probe);
    }

    @Override
    protected void createNewTrip(Probe probe) throws IOException {
        Trip trip = newTripFromProbe(probe);
        updateTrip(trip);
    }

    @Override
    protected Trip findPendingTrip(ByteString sensorId) throws IOException {
        Record record = client.get(null, new Key(NAMESPACE, null, sensorId.toByteArray()));
        if(record != null) {
            byte[] val = (byte[]) record.getValue("");
            return val != null ? Trip.parseFrom(val) : null;
        } else {
            return null;
        }
    }

    @Override
    protected void updateTrip(Trip updated) throws IOException {
        client.put(null, new Key(NAMESPACE, null, updated.getTripId().toByteArray()), new Bin(null, updated.toByteArray()));
    }

    @Override
    protected void finalize() throws Throwable {
        if (client != null)
            client.close();
        super.finalize();
    }

}
