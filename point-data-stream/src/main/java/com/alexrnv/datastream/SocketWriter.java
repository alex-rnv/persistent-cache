package com.alexrnv.datastream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;

/**
 * @author ARyazanov
 *         1/27/2016.
 */
@Component
@ConditionalOnExpression("'${mode}'=='socket'")
public class SocketWriter extends WriterWithStats {

    @Value("${out.host}")
    private String host;
    @Value("${out.port}")
    private int port;

    private volatile BufferedWriter writer;

    @PostConstruct
    protected void init() throws IOException {
        Socket socket = new Socket(host, port);
        this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        super.init();
    }

    @Override
    public void write(String line) {
        try {
            incWriteCounter();
            writer.write(line);
            writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        if(writer != null) {
            writer.close();
        }
        super.finalize();
    }
}
