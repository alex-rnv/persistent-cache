package com.alexrnv.datastream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.concurrent.*;

/**
 * @author ARyazanov
 *         1/27/2016.
 */
@Component
public class EventStream {

    @Value("${input.generated}")
    private boolean generateProbes;

    @Resource
    private StreamConfig config;
    @Resource
    private GeneratingSource source;
    @Resource
    private Writer writer;
    private volatile BlockingQueue<String> queue;
    private final ScheduledExecutorService scheduled = Executors.newSingleThreadScheduledExecutor();
    private final Executor executor = Executors.newSingleThreadExecutor();
   // private volatile List<String> buffer;

    @PostConstruct
    public void init() {
        this.queue = new ArrayBlockingQueue<>(config.queueSize);
        //this.buffer = new ArrayList<>(config.packetSize);
    }

    public void start() {
        int delay = 1000 / config.packetsPerSec;
        scheduled.scheduleAtFixedRate(() -> {
//            synchronized (buffer) {
//                buffer.clear();
//                queue.drainTo(buffer, config.packetSize);
//                write(buffer);
//            }

            int n = Math.min(queue.size(), config.packetSize);
            write(n);
        }, delay, delay, TimeUnit.MILLISECONDS);
        executor.execute(() -> source.produce(this));
    }

    synchronized void add(String string) {
        try {
            queue.put(string);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void write(int n) {
        while (n-- > 0) {
            try {
                writer.write(queue.take());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
