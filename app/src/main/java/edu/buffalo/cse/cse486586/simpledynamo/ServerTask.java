package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.database.Cursor;
import android.os.AsyncTask;
import android.util.Log;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StreamCorruptedException;
import java.lang.reflect.Array;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;


/**
 *
 * Created by sunandan on 3/19/16.
 */
    /*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/
public class ServerTask extends AsyncTask<ServerSocket, String, Void> {
    /**
     * Prints a generic tree set
     * @param treeSet
     */
    public void printTreeSet(DynamoList<String> treeSet) {
        Log.e("printTreeSet","******  begin ******");
        for(String port : treeSet) {
            Log.e("printTreeSet","port " + port);
        }
        Log.e("printTreeSet","******* end  *****");
    }

    /**
     * Finds the location of the message.key
     * Since AVD0 stores the chord ring list, we can use a simple lookup strategy similiar to "doLookup"
     * If the hash(key) is greater / lesser than all chord nodes, then first node stores the data
     * Else the ceil(hash) is used as the strategy to store the data
     * This function retrieves location as per the same logic
     * @param message
     * @return
     */
//    private String findLocationStoringTheMessage(Message message) {
//        String key     = message.key;
//        String hashKey = SimpleDhtProvider.getInstance().genHash(key);
//        if ((hashKey.compareTo(SimpleDhtProvider.getInstance().chordList.first()) < 0) || (hashKey.compareTo(SimpleDhtProvider.getInstance().chordList.last()) > 0 )) {
//            return  SimpleDhtProvider.hashWithPortMap.get(SimpleDhtProvider.getInstance().chordList.first());
//        } else {
//            return SimpleDhtProvider.hashWithPortMap.get(SimpleDhtProvider.getInstance().chordList.ceiling(hashKey));
//        }
//    }

    /**
     * Compare the oldChordList with new chord list to find out any node which had their successor and predecessor changed;
     * Used by AVD0 to notify them of the same.
     * @param
     * @return
     */
//    private HashSet<String> getNodesWithNewPredAndSucc(ChordList<String> oldChordList) {
//        HashSet<String> result = new HashSet<String>();
//        String tempPred, tempSucc;
//
//        for(String node :SimpleDhtProvider.getInstance().chordList) {
//            if (!oldChordList.contains(node))
//            {
//                result.add(node);
//            } else {
//                tempSucc = oldChordList.getSuccessor(node);
//                tempPred = oldChordList.getPredecessor(node);
//
//                if (!tempSucc.equalsIgnoreCase(SimpleDhtProvider.getInstance().chordList.getSuccessor(node))) { //successor is different
//                    result.add(node);
//                }
//                if (!tempPred.equalsIgnoreCase(SimpleDhtProvider.getInstance().chordList.getPredecessor(node))) { //pred is different
//                    result.add(node);
//                }
//            }
//        }
//        return result;
//    }
    private StringBuilder getReplicationCount(Cursor cursor) {
        Log.e("getReplicationCount","Lets see what we get ...");
        if (cursor == null || cursor.getCount() == 0) {
            return null;
        }
        Log.e("getReplicationCount","We can start ...");
        StringBuilder sb = new StringBuilder();
        int replicationIndex = cursor.getColumnIndex(SimpleDynamoActivity.REPLICATE);
        Log.e("getReplicationCount","replicationIndex ..." + replicationIndex);
        int replicationCount = 0;
        cursor.moveToFirst();

        while(!cursor.isAfterLast()) {
            replicationCount = cursor.getInt(replicationIndex);
            Log.e("getReplicationCount","replicationCount ... " + replicationCount);
            sb = sb.append(String.valueOf(replicationCount)).append(":");
            cursor.moveToNext();
        }
        Log.e("getReplicationCount","Final result ..." + sb.toString());
        return sb;
        //return  replicationList;
    }
    /**
     * Extracts all the "key" and "value" fields from database and returns them in a colon-seperated format in the map
     * @param
     * @return
     */
    private HashMap<String,String> getAllKeysAndValues(Cursor cursor) {
        HashMap<String,String> list = new HashMap<String, String>();
        if (cursor == null || cursor.getCount() == 0) {
            return list;
        }
        int keyIndex  = cursor.getColumnIndex(SimpleDynamoActivity.KEY_FIELD);
        int valueIndex= cursor.getColumnIndex(SimpleDynamoActivity.VALUE_FIELD);

        //Log.e("getAllKeysAndValues","key");
        cursor.moveToFirst();
        while(!cursor.isAfterLast()) {
            String key  = cursor.getString(keyIndex);
            String value= cursor.getString(valueIndex);
            Log.e("getAllKeysAndValues","");

            if (list.get(SimpleDynamoActivity.KEY_FIELD) == null) {
                Log.e("getAllKeysAndValues","");
                list.put(SimpleDynamoActivity.KEY_FIELD,key);
            } else {
                Log.e("getAllKeysAndValues","");
                list.put(SimpleDynamoActivity.KEY_FIELD,new StringBuilder(list.get(SimpleDynamoActivity.KEY_FIELD)).append(":").append(key).toString());
            }

            if (list.get(SimpleDynamoActivity.VALUE_FIELD) == null) {
                Log.e("getAllKeysAndValues","");
                list.put(SimpleDynamoActivity.VALUE_FIELD,value);
            } else {
                Log.e("getAllKeysAndValues","");
                list.put(SimpleDynamoActivity.VALUE_FIELD,new StringBuilder(list.get(SimpleDynamoActivity.VALUE_FIELD)).append(":").append(value).toString());
            }

            cursor.moveToNext();
        }
        Log.e("getAllKeysAndValues","");
        return list;
    }

    private ContentValues constructContentValue(Message message) {
        ContentValues cv = new ContentValues();
        cv.put(SimpleDynamoActivity.KEY_FIELD, message.key);
        cv.put(SimpleDynamoActivity.VALUE_FIELD, message.value);
        return cv;
    }

    private void sendReplicationMessageOnRecovery(String[] replArr,String[] keyArr,String[] valArr,Message message) {
        String first_Succ = SimpleDynamoProvider.dynamoList.getSuccessor(
                SimpleDynamoProvider.node_id);
        String second_Succ= SimpleDynamoProvider.dynamoList.getSuccessor(first_Succ);
        message.originPort  = SimpleDynamoProvider.myPort;
        message.messageType = SimpleDynamoProvider.REPLICATE;
        for(int i=0; i < keyArr.length; i++) {

            if (Integer.valueOf(replArr[i]) > 0 ) {
                message.key   = keyArr[i];
                message.value = valArr[i];
                Log.e("sendReplMsgOnRecovery","key " + keyArr[i] + ":" + valArr[i] + ":" + replArr[i]);
                /*Replication 1*/
                message.remotePort  = SimpleDynamoProvider.dynamoList.getPortFromPortHash(first_Succ);
                SimpleDynamoProvider.sendMessageToRemotePort(new Message(message));
                /*Replication 2*/
                message.remotePort  = SimpleDynamoProvider.dynamoList.getPortFromPortHash(second_Succ);
                SimpleDynamoProvider.sendMessageToRemotePort(new Message(message));
            } else {
                Log.e("sendReplMsgOnRecovery","key " + keyArr[i] + " NOT replicated");
            }
        }
    }

    private boolean addSingleRowDataToLocalDB(Cursor cursor,Message message) {
        Log.e("addSingleDataToLocalDB", "called");
        boolean bResult = false;
        if ( (cursor == null) || (cursor.getCount() == 0)) {
            return bResult;
        }
        cursor.moveToFirst();
        int keyIndex  = cursor.getColumnIndex(SimpleDynamoActivity.KEY_FIELD);
        int valueIndex= cursor.getColumnIndex(SimpleDynamoActivity.VALUE_FIELD);

        if (keyIndex == -1 || valueIndex == -1) {
            Log.e("addSingleDataToLocalDB", "Wrong columns");
            return bResult;
        }

        if (!(cursor.isFirst() &&  cursor.isLast())) {
            Log.e("addSingleDataToLocalDB", "Wrong number of rows");
            return bResult;
        }

        String returnKey   = cursor.getString(keyIndex);
        String returnValue = cursor.getString(valueIndex);
        Log.e("addSingleDataToLocalDB", "returnKey " + returnKey + " returnValue " + returnValue);
        if (message.value.equalsIgnoreCase(returnValue)) {
            Log.e("addSingleDataToLocalDB"," Not inserting as data is repeated...");
            return bResult;
        }
        Log.e("addSingleDataToLocalDB","Value " + message.value + " is different from " + returnValue + "... Need to insert this");
        Message temp    = new Message(returnKey,message.value,"dummy","dummy","dummy","dummy"); /*Update the value brother*/
        if (SimpleDynamoProvider.sql.insertValues(constructContentValue(temp)) == -1) {
            Log.e("addSingleDataToLocalDB","addSingleRowDataToLocalDB SQL insertion failed =" + message.toString());
            return bResult;
        }
        bResult = true;
        return bResult;
    }


    private void sendACK(Socket clientSocket) throws Exception{
        PrintWriter printWriter;
        OutputStream outputStream;
        /*Send ACK message*/
        Message tempACK= new Message ("dummy","dummy",SimpleDynamoProvider.HEARTBEAT,SimpleDynamoProvider.myPort,
                String.valueOf(clientSocket.getPort()),"-1"); // in step 1, no sequence number is required to be sent
        outputStream = clientSocket.getOutputStream();
        printWriter  = new PrintWriter(outputStream);
        printWriter.println(tempACK.deconstructMessage());
        printWriter.flush();

    }
    /**
     * Does a lookup on the message's hashkey and determines whether the message to this node, or to forward to successor node for further lookups
     * @return
     */
    @Override
    protected Void doInBackground(ServerSocket... sockets) {
        String TAG = SimpleDynamoProvider.TAG;

        ServerSocket serverSocket = sockets[0];
        Socket clientSocket;
        PrintWriter printWriter;
        BufferedReader reader   = null;
        OutputStream outputStream;

        String msgIncoming;
        try {
            while(true) {
                clientSocket = serverSocket.accept();
                clientSocket.setSoTimeout(SimpleDynamoProvider.TIMEOUT);
                try {
                    /*Send ACK message*/
                    /*Message tempACK= new Message ("dummy","dummy",SimpleDynamoProvider.HEARTBEAT,SimpleDynamoProvider.myPort,
                                                String.valueOf(clientSocket.getPort()),"-1"); // in step 1, no sequence number is required to be sent
                    outputStream = clientSocket.getOutputStream();
                    printWriter  = new PrintWriter(outputStream);
                    printWriter.println(tempACK.deconstructMessage());
                    printWriter.flush();*/
                    sendACK(clientSocket);
                    /*Now wait for actual message*/
                    reader      = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    msgIncoming = reader.readLine();
                    if (!msgIncoming.equals(null) && !msgIncoming.equals("") && (msgIncoming.indexOf("HEARTBEAT")> -1 )) {
                        Log.e(TAG,"msgReceived " + msgIncoming);
                    }

                    Message message = new Message(new String(msgIncoming));

                    //clientSocket.close();
                    /*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/
                    if (message.messageType.equalsIgnoreCase(SimpleDynamoProvider.INSERT)) {
                        //Log.e(TAG, "INSERT message found from  " + message.originPort);
                        /*Check if key already exists into local db, if yes then just replicate*/
                        Cursor cursor = SimpleDynamoProvider.getInstance().returnLocalData(message.key);
                        if (cursor == null) {
                            throw new Exception();
                        }
                        if (cursor.getCount() == 0) {
                            //Log.e(TAG,"TO insert " + message.toString() + " to local DB");
                            if (SimpleDynamoProvider.getInstance().insertIntoDatabase(constructContentValue(message)) == -1) {
                                Log.e("ServerTask","Got failure in ServerTask  for DB insert for message :" + message.toString());
                            } else {
                                Log.e("ServerTask","Message " + message.toString() + " inserted in ServerTask");
                            }
                            /**/

                        }
                        else {
                            /*Now it may happen that during the change of phases, same key with different values are inserted
                            * To catch such cases, we will need to check if the value in DB  == message.value
                            * If not, then update local DB*/

                            addSingleRowDataToLocalDB(cursor,message);
                        }
                            //Log.e(TAG,"Message " + message.toString() + " already in local DB");
                        cursor.close();
                        /*Replicate into 2 next successors here*/
                        String first_Succ = SimpleDynamoProvider.dynamoList.getSuccessor(
                                            SimpleDynamoProvider.node_id);
                        //Log.e(TAG,"first_Succ " + first_Succ);
                        String second_Succ= SimpleDynamoProvider.dynamoList.getSuccessor(first_Succ);
                        //Log.e(TAG,"second_Succ " + second_Succ);
                        /*Replication 1*/

                        message.originPort  = SimpleDynamoProvider.myPort;
                        message.remotePort  = SimpleDynamoProvider.dynamoList.getPortFromPortHash(first_Succ);
                        //Log.e(TAG,"First succ port " + message.remotePort);
                        message.messageType = SimpleDynamoProvider.REPLICATE;

                         SimpleDynamoProvider.sendMessageToRemotePort(new Message(message));
                        /*Replication 2*/
                        message.remotePort  = SimpleDynamoProvider.dynamoList.getPortFromPortHash(second_Succ);
                        ///Log.e(TAG,"Second succ port " + message.remotePort);
                        SimpleDynamoProvider.sendMessageToRemotePort(new Message(message));

                    } else if (message.messageType.equalsIgnoreCase(SimpleDynamoProvider.REPLICATE)) {
                        Log.e(TAG,"REPLICATE message " + message.toString() + " received ");
                        if (SimpleDynamoProvider.getInstance().insertIntoDatabase(constructContentValue(message)) == -1) {
                            Log.e("ServerTask","Failed in DB insert for REPLICATE");
                        }

                    } else if (message.messageType.equalsIgnoreCase(SimpleDynamoProvider.QUERY_GET_DATA)) {
                        /*Used by remote ports to ask for data from the database, "key" can be either specific text, or "@" parameter*/
                        Log.e(TAG, "QUERY_GET_DATA, To query " + message.key);
                        Cursor cursor = SimpleDynamoProvider.getInstance().returnLocalData(message.key);
                        if (cursor == null) {
                            throw new Exception();
                        }

                        message.messageType = SimpleDynamoProvider.QRY_DATA_DONE;
                        HashMap<String,String> list = getAllKeysAndValues(cursor);

                        if (list.size() > 0) { //message.key will have all the key fields in colon-seperated format if parameter was GDump parameter
                            message.key   = list.get(SimpleDynamoActivity.KEY_FIELD);
                            message.value = list.get(SimpleDynamoActivity.VALUE_FIELD);
                        } else { //safety condition
                            message.key   = SimpleDynamoProvider.EMPTY;
                            message.value = SimpleDynamoProvider.EMPTY;
                        }

                        Log.e(TAG,"DB key " + message.key);
                        Log.e(TAG,"DB val " + message.value);

                        message.remotePort  = message.originPort;
                        message.originPort  = SimpleDynamoProvider.myPort;
                        cursor.close();
                        SimpleDynamoProvider.sendMessageToRemotePort(message);

                    }  else if (message.messageType.equalsIgnoreCase(SimpleDynamoProvider.QRY_DATA_DONE)) {
                        /*Originator of Query lookup request receives this response and unlocks waiting content provider for processing*/
                        Log.e(TAG,"Result of QRY_DATA_DONE from " + message.originPort + " is " + " key=" + message.key + " = " + message.value);

                        SimpleDynamoProvider.queryKey  = message.key;
                        SimpleDynamoProvider.queryValue= message.value;
                        synchronized (SimpleDynamoProvider.lock) {
                            SimpleDynamoProvider.queryDone = true;
                            SimpleDynamoProvider.lock.notifyAll();
                        }
                    } else if (message.messageType.equalsIgnoreCase(SimpleDynamoProvider.RECOVER)) {
                        /*Check if there is any message in failure DB for the remote port
                        * If yes, then send it over
                        * If no, then just ignore the message*/
                        Log.e(TAG,"RECOVER message from " + message.originPort);
                        SimpleDynamoProvider.failedAVDList.remove(message.originPort);
                        Cursor cursor = SimpleDynamoProvider.getInstance().returnFailureTableDataForRemote(message.originPort);
                        if (cursor != null ) {
                            message.messageType  = SimpleDynamoProvider.RECOVER_RESP;
                            HashMap<String,String> list = getAllKeysAndValues(cursor);
                            StringBuilder replicationList = getReplicationCount(cursor);

                            if (list.size() > 0) { //message.key will have all the key fields in colon-seperated format if parameter was GDump parameter
                                message.key   = list.get(SimpleDynamoActivity.KEY_FIELD);
                                message.value = list.get(SimpleDynamoActivity.VALUE_FIELD);
                                message.replicationCount = replicationList.toString();
                                Log.e(TAG,"RECOVERY response DB key " + message.key);
                                Log.e(TAG,"RECOVERY response DB val " + message.value);
                                Log.e(TAG,"RECOVERY response repl count " + message.replicationCount );
                            } else { //safety condition
                                message.key   = SimpleDynamoProvider.EMPTY;
                                message.value = SimpleDynamoProvider.EMPTY;
                                Log.e(TAG,"No data found for recovery");
                            }
                            /*
                            * Delete all the recovered keys from the failure table, no need to keep them now
                            */

                            message.remotePort  = message.originPort;
                            message.originPort  = SimpleDynamoProvider.myPort;

                            cursor.close();
                            //Log.e(TAG,"To inform " + message.remotePort + " for recovery message");
                            SimpleDynamoProvider.sendMessageToRemotePort(message);
                            Log.e(TAG,"Remove from failureTable for "  + message.remotePort);
                            SimpleDynamoProvider.deleteFromFailureTable(message.key);
                        } else
                            Log.e(TAG,"");

                    } else if (message.messageType.equalsIgnoreCase(SimpleDynamoProvider.RECOVER_RESP)) {
                        SimpleDynamoProvider.recoveryCount++;
                        Log.e(TAG,"RECOVER_RESP found from " + message.originPort);
                        if (!message.key.equalsIgnoreCase(SimpleDynamoProvider.EMPTY) &&
                            !message.value.equalsIgnoreCase(SimpleDynamoProvider.EMPTY)    ) {

                            Log.e(TAG,"message.key "+ message.key);
                            Log.e(TAG,"message.value " + message.value);
                            Log.e(TAG,"message.replicationCount " + message.replicationCount);

                            String[] keyArr = message.key.split(":");
                            String[] valArr = message.value.split(":");
                            String[] replArr= message.replicationCount.split(":");
                            Log.e(TAG,"Length Keys from port " + message.originPort + " = "   + keyArr.length);
                            Log.e(TAG,"Length values from port " + message.originPort + " = " + valArr.length);
                            Log.e(TAG,"Length replArr from port " + message.originPort + " = " + replArr.length);
                            int replicationCount = 0;
                            /*First insert the data into local DB*/
                            for(int i=0; i < keyArr.length; i++) {
                                Log.e(TAG,"RECOVER_RESP keys "  + keyArr[i] + ":" + valArr[i]);
                                ContentValues contentValues = new ContentValues();
                                contentValues.put(SimpleDynamoActivity.KEY_FIELD,keyArr[i]);
                                contentValues.put(SimpleDynamoActivity.VALUE_FIELD,valArr[i]);

                                if (SimpleDynamoProvider.getInstance().insertIntoDatabase(contentValues) == -1) {
                                    Log.e("ServerTask","Failed in DB insert for RECOVER_RESP");
                                }
                                //String[] results  = new String[]{keyArr[i],valArr[i]};
                                //matrixCursor.addRow(results);
                            }
                            /*Now send out replication messages*/
                            Log.e(TAG,"Now send recovery messages (if required)...");
                            sendReplicationMessageOnRecovery(replArr,keyArr,valArr,message);
                        } else {
                            Log.e(TAG,"No keys found from " + message.originPort);
                        }
                        Log.e(TAG,"failedAVDLiist " + SimpleDynamoProvider.failedAVDList.size());
                        for(String s: SimpleDynamoProvider.failedAVDList) {
                            Log.e(TAG,"failed AVD " + s);
                        }
                        if (SimpleDynamoProvider.recoveryCount == (SimpleDynamoProvider.REMOTE_PORT.length - SimpleDynamoProvider.failedAVDList.size() - 1)) {
                            //we avoid sending message to the same port, hence the "-1"
                            Log.e(TAG,"Recovery completed");
                            SimpleDynamoProvider.isRecoveryDone = true;
                            synchronized (SimpleDynamoProvider.recoveryLock) {
                                SimpleDynamoProvider.recoveryLock.notifyAll();
                            }
                        } else {
                            Log.e(TAG,"Recovery remains... response found from  " + SimpleDynamoProvider.recoveryCount);
                        }
                    }
                    /*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/
                    else if (message.messageType.equalsIgnoreCase(SimpleDynamoProvider.DELETE_DATA)) {
                        Log.e(TAG,"Delete request found from " + message.originPort + " selection parameter "  + message.key );
                        SimpleDynamoProvider.sql.deleteDataFromTable(message.key);

                    }
                    sendACK(clientSocket);
                    clientSocket.close();

                } catch(SocketTimeoutException ex) {
                    ex.printStackTrace();
                    Log.e(TAG, "process " + clientSocket.getPort() + " is dead !! ");
                    SimpleDynamoProvider.handleFailures(String.valueOf(clientSocket.getPort()) , null);
                    Log.e(TAG, "ClientTask SocketTimeoutException " + ex.getMessage());
                } catch (UnknownHostException ex) {
                    ex.printStackTrace();
                    Log.e(TAG, "process " + String.valueOf(clientSocket.getPort()) + " is dead !! ");
                    SimpleDynamoProvider.handleFailures(String.valueOf(clientSocket.getPort()), null);
                    Log.e(TAG, "ClientTask UnknownHostException " + ex.getMessage());
                } catch( IOException ex) {
                    Log.e(TAG, "process " + String.valueOf(clientSocket.getPort()) + " is dead !! ");
                    SimpleDynamoProvider.handleFailures(String.valueOf(clientSocket.getPort()), null);
                    ex.printStackTrace();
                    Log.e(TAG, "ClientTask socket IOException " + ex.getMessage());
                } catch (Exception ex) {
                    ex.printStackTrace();
                    Log.e(TAG, "process " + String.valueOf(clientSocket.getPort()) + " is dead !! ");
                    SimpleDynamoProvider.handleFailures(String.valueOf(clientSocket.getPort()), null);
                    Log.e(TAG, "ClientTask socket Exception " + ex.getMessage());
                }


            }

        } catch(Exception e) {
            e.printStackTrace();
        }
        try {
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
