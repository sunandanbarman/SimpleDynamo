package edu.buffalo.cse.cse486586.simpledynamo;

import android.os.AsyncTask;
import android.util.Log;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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
        DataOutputStream dataOutputStream;
        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.valueOf(message.remotePort));

            socket.setSoTimeout(SimpleDynamoProvider.TIMEOUT);
            socket.setSoTimeout(SimpleDynamoProvider.TIMEOUT);
            outputStream    = socket.getOutputStream();
            dataOutputStream= new DataOutputStream(outputStream);
            String tempMsg  = message.deconstructMessage();

            dataOutputStream.write(tempMsg.getBytes());
            dataOutputStream.flush();

            socket.close();
        } catch(SocketTimeoutException ex) {
            ex.printStackTrace();
        } catch (UnknownHostException ex) {
            ex.printStackTrace();
        } catch( IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }
 }
