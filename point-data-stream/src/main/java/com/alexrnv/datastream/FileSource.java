package com.alexrnv.datastream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author ARyazanov
 *         1/27/2016.
 */
@Component
@Deprecated
public class FileSource {

    @Value("${source.folder}")
    private String folder;

    private final Executor async = Executors.newSingleThreadExecutor();
    //second reader to open file descriptors in advance, so reading a folder should look like reading one big file
    private final AtomicReference<FileReader> secondReader = new AtomicReference<>();
    private volatile FileReader currentReader = null;

    public void produce(EventStream stream) {

        File[] files = new File(folder).listFiles((dir, name) -> {
            return name.endsWith(".tsv");
        });
        Arrays.sort(files, (f1, f2) -> f1.getName().compareTo(f2.getName()));

        int i=0;
        try {
            openNextFile(files[0]);
        } catch (FileNotFoundException e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }

        while(i < files.length) {
            currentReader = secondReader.getAndSet(null);
            while(currentReader == null) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace(System.out);
                }
                currentReader = secondReader.getAndSet(null);
            }
            i++;
            if(i < files.length) {
                final File nextFile = files[i];
                async.execute(() -> {
                    try {
                        System.out.println("Open file " + nextFile.getName());
                        openNextFile(nextFile);
                    } catch (FileNotFoundException e) {
                        e.printStackTrace(System.out);
                    }
                });
            }
            try(BufferedReader reader = new BufferedReader(currentReader)) {
                String line;
                while((line = reader.readLine()) != null) {
                    stream.add(line);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace(System.out);
                System.exit(1);
            } catch (IOException e) {
                e.printStackTrace(System.out);
                System.exit(1);
            }
        }
        System.out.println("Finished");
    }

    private void openNextFile(File file) throws FileNotFoundException {
        secondReader.set(new FileReader(file));
    }
}
