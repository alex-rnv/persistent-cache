package com.alexrnv.datastream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @author ARyazanov
 *         1/27/2016.
 */
@Component
public class StreamConfig {
    @Value("${packets.per.sec}")
    int packetsPerSec;
    @Value("${packet.size}")
    int packetSize;
    @Value("${queue.size}")
    int queueSize;
}
