package com.alexrnv.tripgen.front;

import com.alexrnv.tripgen.convert.ProbeConverter;
import com.alexrnv.tripgen.convert.TsvProbeConverter;
import com.alexrnv.tripgen.dto.DataObjects;
import com.alexrnv.tripgen.workflow.TripsWorkFlow;
import com.alexrnv.tripgen.workflow.WorkFlowStats;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created: 2/10/16 6:38 PM
 * Author: alex
 */
public class SocketFront {

    private final ProbeConverter<String> probeConverter = new TsvProbeConverter();
    private final int port;
    private final TripsWorkFlow workFlow;
    private final WorkFlowStats workFlowStats;
    //protected final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
    protected final ForkJoinPool mainExecutor;
    protected final ScheduledExecutorService miscExecutor = Executors.newScheduledThreadPool(2);

    public SocketFront(int port, TripsWorkFlow workFlow) {
        this.port = port;
        this.workFlow = workFlow;
        this.workFlowStats = workFlow.getWorkFlowStats();
        //this.mainExecutor = new ThreadPoolExecutor(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS, queue);
        this.mainExecutor = new ForkJoinPool(Runtime.getRuntime().availableProcessors(),
                ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);
        System.out.println("Socket front started on port " + port);
    }

    public void listen() {

        this.miscExecutor.scheduleAtFixedRate(() -> {
            //System.out.println(String.format("Current queue size is %s", queue.size()));
            System.out.println(String.format("Current queue size is %s",  mainExecutor.getQueuedSubmissionCount()));
        }, 12, 10, TimeUnit.SECONDS);

        this.miscExecutor.scheduleAtFixedRate(() -> {
            try {
                int created = workFlowStats.getAndResetCreated();
                int updated = workFlowStats.getAndResetUpdated();
                int completed = workFlowStats.getAndResetCompleted();
                double fMean = workFlowStats.getMeanTimeFind();
                double cMean = workFlowStats.getMeanTimeCreate();
                double uMean = workFlowStats.getMeanTimeUpdate();
                double cmpMean = workFlowStats.getMeanTimeComplete();
                double allMean = workFlowStats.getMeanTimeAll();
                System.out.println(String.format("Counters: created %d, updated %d, completed %d, all %d",
                        created, updated, completed, (created + updated + completed)));
                System.out.println(String.format("Timing (mean, ns): find %f, create %f, update %f, complete %f, all %f",
                        fMean, cMean, uMean, cmpMean, allMean));
            } catch (Exception e) {
               e.printStackTrace(System.out);
            }
        }, 10, 10, TimeUnit.SECONDS);

        int lineCounter = 0;
        try (ServerSocket serverSocket = new ServerSocket(port);
             Socket clientSocket = serverSocket.accept();
             InputStreamReader is = new InputStreamReader(clientSocket.getInputStream());
             BufferedReader reader = new BufferedReader(is)
        ) {
            String line;
            while((line = reader.readLine()) != null) {
                final String l = line;
                mainExecutor.execute(() -> {
                    try {
                        DataObjects.Probe probe = probeConverter.convert(l);
                        workFlow.processProbe(probe);
                    } catch (Exception e) {
                        e.printStackTrace(System.out);
                    }
                });
                lineCounter++;
                if(lineCounter % 1000000 == 0) {
                    System.out.println("Lines processed " + lineCounter);
                }
            }

        } catch (IOException e) {
            e.printStackTrace(System.out);
        }
    }
}
