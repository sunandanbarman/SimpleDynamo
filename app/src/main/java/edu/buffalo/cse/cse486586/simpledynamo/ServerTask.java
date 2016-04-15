package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.database.Cursor;
import android.os.AsyncTask;
import android.util.Log;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StreamCorruptedException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
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
     * @param oldChordList
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

    /**
     * Extracts all the "key" and "value" fields from database and returns them in a colon-seperated format in the map
     * @param
     * @return
     */
//    private HashMap<String,String> getAllKeysAndValues(Cursor cursor) {
//        HashMap<String,String> list = new HashMap<String, String>();
//        if (cursor == null || cursor.getCount() == 0) {
//            return list;
//        }
//        int keyIndex  = cursor.getColumnIndex(SimpleDhtActivity.KEY_FIELD);
//        int valueIndex= cursor.getColumnIndex(SimpleDhtActivity.VALUE_FIELD);
//
//        cursor.moveToFirst();
//        while(!cursor.isAfterLast()) {
//            String key  = cursor.getString(keyIndex);
//            String value= cursor.getString(valueIndex);
//            if (list.get(SimpleDhtActivity.KEY_FIELD) == null) {
//                list.put(SimpleDhtActivity.KEY_FIELD,key);
//            } else {
//                list.put(SimpleDhtActivity.KEY_FIELD,new StringBuilder(list.get(SimpleDhtActivity.KEY_FIELD)).append(":").append(key).toString());
//            }
//
//            if (list.get(SimpleDhtActivity.VALUE_FIELD) == null) {
//                list.put(SimpleDhtActivity.VALUE_FIELD,value);
//            } else {
//                list.put(SimpleDhtActivity.VALUE_FIELD,new StringBuilder(list.get(SimpleDhtActivity.VALUE_FIELD)).append(":").append(value).toString());
//            }
//
//            cursor.moveToNext();
//        }
//        return list;
//    }

    private ContentValues constructContentValue(Message message) {
        ContentValues cv = new ContentValues();
        cv.put(SimpleDynamoActivity.KEY_FIELD, message.key);
        cv.put(SimpleDynamoActivity.VALUE_FIELD, message.value);
        return cv;
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
        InputStream inputStream;
        DataInputStream dataInputStream;
        byte[] msgIncoming = new byte[SimpleDynamoProvider.MAX_MSG_LENGTH];

        try {
            while(true) {
                clientSocket = serverSocket.accept();
                try {
                    inputStream = clientSocket.getInputStream();
                    dataInputStream = new DataInputStream(inputStream);
                    dataInputStream.read(msgIncoming);

                    Message message = new Message(new String(msgIncoming));
                    if (!msgIncoming.equals(null) && msgIncoming.equals("")) {
                        Log.e(TAG,"msgReceived " + msgIncoming);
                    }
                    clientSocket.close(); //release the connection, process the message
                    //Log.e(TAG," message t");
                    /*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/
                    if (message.messageType.equalsIgnoreCase(SimpleDynamoActivity.INSERT)) {
                        Log.e(TAG, "INSERT message found from  " + message.originPort);
                        /*ContentValues cv = new ContentValues();
                        cv.put(SimpleDynamoActivity.KEY_FIELD, message.key);
                        cv.put(SimpleDynamoActivity.VALUE_FIELD, message.value);
                        */
                        SimpleDynamoProvider.insertIntoDatabase(constructContentValue(message));
                        /*Replicate into 2 next successors here*/
                        String first_Succ = SimpleDynamoProvider.dynamoList.getSuccessor(
                                            SimpleDynamoProvider.node_id);
                        Log.e(TAG,"first_Succ " + first_Succ);
                        String second_Succ= SimpleDynamoProvider.dynamoList.getSuccessor(first_Succ);
                        Log.e(TAG,"second_Succ " + second_Succ);
                        /*Replication 1*/
                        message.originPort  = SimpleDynamoProvider.myPort;
                        message.remotePort  = SimpleDynamoProvider.dynamoList.getPortFromPortHash(first_Succ);
                        Log.e(TAG,"First succ port " + message.remotePort);
                        message.messageType = SimpleDynamoActivity.REPLICATE;
                        SimpleDynamoProvider.sendMessageToRemotePort(message);
                        /*Replication 2*/
                        message.remotePort  = SimpleDynamoProvider.dynamoList.getPortFromPortHash(second_Succ);
                        Log.e(TAG,"Second succ port " + message.remotePort);
                        SimpleDynamoProvider.sendMessageToRemotePort(message);
                    } else if (message.messageType.equalsIgnoreCase(SimpleDynamoActivity.REPLICATE)) {
                        SimpleDynamoProvider.insertIntoDatabase(constructContentValue(message));

                    }
//                    if (message.messageType.equalsIgnoreCase(SimpleDhtProvider.ALIVE)) { //New node joined chord ring
//                        Log.e(TAG, "ALIVE message found from " + message.originPort);
//                        Log.e(TAG, "to add " + message.originPort);
//
//                        ChordList<String> tempList = new ChordList<String>();
//                        tempList.addAll(new TreeSet<String>(SimpleDhtProvider.getInstance().chordList));
//
//                        /*add the new port to chord list; being a tree set, the chord list automatically sorts the list as per hash(port)
//                        portWithHashMap gets the correct hash, this is statically created so that we can use it for all sorts of tasks later*/
//                        SimpleDhtProvider.getInstance().chordList.add(SimpleDhtProvider.getInstance().portWithHashMap.get(message.originPort));
//
//                        HashSet<String> nodeHashList = getNodesWithNewPredAndSucc(tempList);
//                        for (String node: nodeHashList) {
//                            String predPort = SimpleDhtProvider.getInstance().hashWithPortMap.get(SimpleDhtProvider.getInstance().chordList.getPredecessor(node));
//                            String succPort = SimpleDhtProvider.getInstance().hashWithPortMap.get(SimpleDhtProvider.getInstance().chordList.getSuccessor(node));
//                            String nodePort = SimpleDhtProvider.getInstance().hashWithPortMap.get(node);
//                            Log.e(TAG,"node " + nodePort + " pred & succ changed !");
//                            Log.e(TAG,"new Pred " + predPort);
//                            Log.e(TAG, "new succ " + succPort);
//                            if (!nodePort.equalsIgnoreCase(SimpleDhtProvider.myPort)) {
//                                Message nodeAdded_msg = new Message("dummy","dummy","dummy",
//                                        SimpleDhtProvider.getInstance().NODE_ADDED,
//                                        SimpleDhtProvider.getInstance().myPort, //origin port
//                                        nodePort,                     //remote port
//                                        predPort, //pred
//                                        succPort);  //succ
//
//                                SimpleDhtProvider.getInstance().sendMessageToRemotePort(nodeAdded_msg);
//                            } else {
//                                SimpleDhtProvider.getInstance().setPredAndSuccHash(predPort, succPort);
//                            }
//                        }
//                    }
//                    else if (message.messageType.equalsIgnoreCase(SimpleDhtProvider.getInstance().NODE_ADDED)) {
//                        /*This is ACK from chord ring master that some new AVD has been added to ring; update predPort and succPort as per the message parameters*/
//                        Log.e(TAG, "Node added ACK found, pred and succ are" + message.predPort + " : " + message.succPort);
//                        SimpleDhtProvider.getInstance().setPredAndSucc(message);
//                    }
//                    /*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/
//                    else if (message.messageType.equalsIgnoreCase(SimpleDhtProvider.INSERT_LOOKUP)) { //route message for inserting message
//                        Log.e(TAG, "Insert_Lookup request found from  " + message.originPort);
//                        ContentValues cv = new ContentValues();
//                        cv.put(SimpleDhtActivity.KEY_FIELD,message.key);
//                        cv.put(SimpleDhtActivity.VALUE_FIELD, message.value);
//
//                        cv.put(SimpleDhtActivity.PORT, message.originPort);
//                        SimpleDhtProvider.getInstance().insert(SimpleDhtActivity.contentURI, cv);
//
//                    }
//                    else if (message.messageType.equalsIgnoreCase(SimpleDhtProvider.QUERY_LOOKUP)) {
//                        Log.e(TAG,"Query_Lookup request found from  " + message.originPort);
//                        String portLocation = findLocationStoringTheMessage(message);
//                        //reply to origin port with found portLocation
//                        Log.e(TAG,"actual port location is " + portLocation);
//
//                        message.remotePort= message.originPort; //reply to sender about it
//                        message.messageType= SimpleDhtProvider.QUERY_DONE;
//                        message.originPort = portLocation;      // this is the location of DB; receiver extracts this for further lookup
//
//                        SimpleDhtProvider.getInstance().sendMessageToRemotePort(message);
//                    } else if (message.messageType.equalsIgnoreCase(SimpleDhtProvider.QUERY_DONE)) {
//                        /*originator of QUERY_LOOKUP receives this response, release the waiting content provider for further processing*/
//                        Log.e(TAG, "Received reply from AVD0 about portLocation " + message.originPort);
//                        SimpleDhtProvider.portLocation = message.originPort;
//                        synchronized (SimpleDhtProvider.lock) {
//                            SimpleDhtProvider.queryDone = true;
//                            SimpleDhtProvider.lock.notifyAll();
//                        }
//
//                    } else if (message.messageType.equalsIgnoreCase(SimpleDhtProvider.QUERY_GET_DATA)) {
//                        /*Used by remote ports to ask for data from the database, "key" can be either specific text, or "@" parameter*/
//                        Log.e(TAG, "QUERY_GET_DATA, To query " + message.key);
//                        Cursor cursor = SimpleDhtProvider.getInstance().returnLocalData(message.key);
//                        if (cursor == null) {
//                            throw new Exception();
//                        }
//
//                        message.messageType = SimpleDhtProvider.QRY_DATA_DONE;
//                        HashMap<String,String> list = getAllKeysAndValues(cursor);
//
//                        if (list.size() > 0) { //message.key will have all the key fields in colon-seperated format if parameter was GDump parameter
//                            message.key   = list.get(SimpleDhtActivity.KEY_FIELD);
//                            message.value = list.get(SimpleDhtActivity.VALUE_FIELD);
//                        } else { //safety condition
//                            message.key   = SimpleDhtProvider.EMPTY;
//                            message.value = SimpleDhtProvider.EMPTY;
//                        }
//
//                        Log.e(TAG,"DB key " + message.key);
//                        Log.e(TAG,"DB val " + message.value);
//
//                        message.remotePort  = message.originPort;
//                        message.originPort  = SimpleDhtProvider.myPort;
//                        cursor.close();
//                        SimpleDhtProvider.getInstance().sendMessageToRemotePort(message);
//                    }
//                    else if (message.messageType.equalsIgnoreCase(SimpleDhtProvider.QRY_DATA_DONE)) {
//                        /*Originator of Query lookup request receives this response and unlocks waiting content provider for processing*/
//                        Log.e(TAG,"Result of QRY_DATA_DONE from " + message.originPort + " is " + " key=" + message.key + " = " + message.value);
//
//                        SimpleDhtProvider.queryKey  = message.key;
//                        SimpleDhtProvider.queryValue= message.value;
//                        synchronized (SimpleDhtProvider.lock) {
//                            SimpleDhtProvider.queryDone = true;
//                            SimpleDhtProvider.lock.notifyAll();
//                        }
//                    }
//                    /*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/
//                    else if (message.messageType.equalsIgnoreCase(SimpleDhtProvider.ALIVE_NODES)) {
//                        Log.e(TAG,"Return all alive nodes list to " + message.originPort);
//                        message.key = SimpleDhtProvider.getInstance().getListOfAliveNodes();
//                        Log.e(TAG,"Alive nodes are " + message.key);
//
//                        message.remotePort = message.originPort;
//                        message.originPort = SimpleDhtProvider.myPort;
//                        message.messageType= SimpleDhtProvider.ALIVE_NODES_RESP;
//
//                        SimpleDhtProvider.getInstance().sendMessageToRemotePort(message);
//
//                    } else if (message.messageType.equalsIgnoreCase(SimpleDhtProvider.ALIVE_NODES_RESP)) {
//                        Log.e(TAG,"List of alive nodes found from " + SimpleDhtProvider.NODE_JOINER_PORT + " is " + message.key);
//                        SimpleDhtProvider.queryKey = message.key;
//                        synchronized (SimpleDhtProvider.lock) {
//                            SimpleDhtProvider.queryDone = true;
//                            SimpleDhtProvider.lock.notifyAll();
//                        }
//                    }
//                    /*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/
//                    else if (message.messageType.equalsIgnoreCase(SimpleDhtProvider.DELETE_DATA)) {
//                        Log.e(TAG,"Delete request found from " + message.originPort + " selection parameter "  + message.key );
//                        SimpleDhtActivity.sql.deleteDataFromTable(message.key);
//
//                    }

                } catch(SocketTimeoutException e) {
                    e.printStackTrace();
                } catch(StreamCorruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }  catch (Exception e) {
                    e.printStackTrace();
                }


            }
        } catch(Exception e) {

        }
        return null;
    }

}
