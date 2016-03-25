package com.alexrnv.tripgen;

import com.alexrnv.tripgen.front.SocketFront;
import com.alexrnv.tripgen.workflow.AerospikeTripsWorkflowImpl;
import com.alexrnv.tripgen.workflow.RocksDBTripsWorkFlowImpl;
import com.alexrnv.tripgen.workflow.TripsWorkFlow;

/**
 * Created: 2/10/16 7:36 PM
 * Author: alex
 */
public class TripGenerator {

    public static void main(String[] args) {
        try {
            String mode = "aerospike";
            if(args.length > 0) {
                mode = args[0];
            }
            int port = 8099;
            if (args.length > 1) {
                port = Integer.parseInt(args[1]);
            }
            String dbPath ="/sd1/rocksdb";
            if(args.length > 2) {
                dbPath = args[2];
            }

            TripsWorkFlow workFlow;
            if("rocksdb".equals(mode)) {
                workFlow = new RocksDBTripsWorkFlowImpl(dbPath);
            } else if("aerospike".equals(mode)) {
                workFlow = new AerospikeTripsWorkflowImpl();
            } else {
                throw new RuntimeException("Wrong mode " + mode);
            }
            SocketFront front = new SocketFront(port, workFlow);

            front.listen();
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
    }
}
