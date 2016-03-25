package com.alexrnv.datastream;

import com.google.common.collect.Iterators;
import org.springframework.beans.factory.annotation.Value;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created: 2/24/16 10:54 AM
 * Author: alex
 */
public abstract class WriterWithStats implements Writer {

    @Value("${stats.folder}")
    private String folder;
    @Value("${stats.flush.period.sec}")
    private int statFlushPeriod;

    private final AtomicInteger writesCounter = new AtomicInteger();
    private volatile int fileCounter;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    private final List<List<Integer>> buffers = new ArrayList<>(2);
    private final Iterator<List<Integer>> bufferIterator = Iterators.cycle(buffers);
    private volatile List<Integer> curBuffer;

    protected void init() throws IOException {

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd_hhmm");
        Path path = Paths.get(folder, formatter.format(LocalDateTime.now()));
        path.toFile().mkdirs();
        folder = path.toString();

        buffers.add(new ArrayList<>((int)1.2 * statFlushPeriod));
        buffers.add(new ArrayList<>((int)1.2 * statFlushPeriod));
        this.curBuffer = bufferIterator.next();

        this.scheduledExecutorService.scheduleAtFixedRate(() ->
                curBuffer.add(writesCounter.getAndSet(0)), 5, 1, TimeUnit.SECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            List<Integer> prevBuffer = curBuffer;
            curBuffer = bufferIterator.next();
            String fileName = String.format("writes_per_sec_%04d.txt", fileCounter++);
            try(BufferedWriter writer = Files.newBufferedWriter(Paths.get(folder, fileName))) {
                for(Integer i : prevBuffer) {
                    writer.write(i.toString());
                    writer.newLine();
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace(System.out);
            } catch (IOException e) {
                e.printStackTrace(System.out);
            }
            prevBuffer.clear();
        }, statFlushPeriod, statFlushPeriod, TimeUnit.SECONDS);
    }

    protected void incWriteCounter() {
        writesCounter.incrementAndGet();
    }
}
