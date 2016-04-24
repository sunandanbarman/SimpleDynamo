package edu.buffalo.cse.cse486586.simpledynamo;

import android.os.AsyncTask;
import android.util.Log;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

/**
 * Generic client task used to establish one-way communication to message.remotePort
 * Does not wait for write results, hence this is a mimic of one way TCP write communication
 * Created by sunandan on 3/19/16.
 */
public class ClientTask extends AsyncTask<Message,Void,Void> {
    @Override
    public Void doInBackground(Message...messages) {
        String TAG = SimpleDynamoProvider.TAG;
        Message message = messages[0];
        if (message.messageType == null || message.messageType.equalsIgnoreCase("")) {
            Log.e(SimpleDynamoProvider.TAG, "messageType is either blank or NULL");
            return null;
        }
        Socket socket;
        OutputStream outputStream;
        PrintWriter printWriter;
        BufferedReader reader = null;
        String msgIncoming = "";
//        DataOutputStream dataOutputStream;
        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.valueOf(message.remotePort));

            socket.setSoTimeout(SimpleDynamoProvider.TIMEOUT);
            //socket.setSoTimeout(SimpleDynamoProvider.TIMEOUT);

            reader      = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            msgIncoming = reader.readLine();
            if (!msgIncoming.equals(null) && !msgIncoming.equals("")) {
                Log.e(TAG,"CLIENT TASK msgReceived " + msgIncoming);
            }
            Message temp = new Message();
            temp.reconstructMessage(msgIncoming);
            Log.e(TAG,"ACK found " + temp.toString());

            outputStream = socket.getOutputStream();
            printWriter  = new PrintWriter(outputStream);
            printWriter.println(message.deconstructMessage());
            printWriter.flush();

            socket.close();
        } catch(SocketTimeoutException ex) {
            ex.printStackTrace();
            Log.e(TAG, "process " + message.remotePort + " is dead !! ");
            SimpleDynamoProvider.handleFailures(message.remotePort,message);
            Log.e(TAG, "ClientTask SocketTimeoutException " + ex.getMessage());
        } catch (UnknownHostException ex) {
            ex.printStackTrace();
            Log.e(TAG, "process " + message.remotePort + " is dead !! ");
            SimpleDynamoProvider.handleFailures(message.remotePort,message);
            Log.e(TAG, "ClientTask UnknownHostException " + ex.getMessage());
        } catch( IOException ex) {
            Log.e(TAG, "process " + message.remotePort + " is dead !! ");
            SimpleDynamoProvider.handleFailures(message.remotePort,message);
            ex.printStackTrace();
            Log.e(TAG, "ClientTask socket IOException " + ex.getMessage());
        } catch (Exception ex) {
            ex.printStackTrace();
            Log.e(TAG, "process " + message.remotePort + " is dead !! ");
            SimpleDynamoProvider.handleFailures(message.remotePort,message);
            Log.e(TAG, "ClientTask socket Exception " + ex.getMessage());
        }

        return null;
    }
 }
