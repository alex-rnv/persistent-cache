package com.alexrnv.datastream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

/**
 * @author ARyazanov
 *         1/27/2016.
 */
@SpringBootApplication
public class EventStreamApp {
    public static void main(String[] args) {
        try {
            ApplicationContext ctx = SpringApplication.run(EventStreamApp.class, args);
            EventStream stream = ctx.getBean(EventStream.class);
            stream.start();
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
    }
}
