package com.alexrnv.datastream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * For testing. Started with one parameter - port to listen at.
 * @author ARyazanov
 *         1/27/2016.
 */
public class SocketListener {

    public static void main1(String[] args) {
        try(ServerSocket socket = new ServerSocket(Integer.valueOf(args[0]));
            Socket clientSocket = socket.accept();
            InputStreamReader i = new InputStreamReader(clientSocket.getInputStream());
            BufferedReader reader = new BufferedReader(i)) {
            clientSocket.setKeepAlive(true);
            String line;
            long k=0, t = System.currentTimeMillis();
            while((line = reader.readLine()) != null) {
                //System.out.println(line);
                k++;
                if(k % 500_0 == 0) System.out.printf("%d  %d\n", System.currentTimeMillis()-t, k);
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
