package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.net.ServerSocket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

/**
 * For phase 1 : Almost all the operations remain the same as Chord DHT assignment, except that instead of inserting
 * into only one AVD, replication needs to be done in the next 2 AVDs as well.
 *
 * For phase2 and phase 3: Ensuring that the DB is a singleton class, and only one copy of DB is opened for read & write
 * plus ensuring thread-safety of DB operations is enough to ensure that these 2 phases pass.
 *
 *
 */
public class SimpleDynamoProvider extends ContentProvider {
	public static String TAG  = SimpleDynamoProvider.class.getName();
    public static final int[] AVD_LIST       = {5554,5556,5558,5560,5562};
    public static final int[] REMOTE_PORT    = {11108,11112,11116,11120,11124};
    public static DynamoList<String> dynamoList;

    public static String predHash = "", succHash = ""; //String having hash(predPort) / hash(succPort)
    public static int predPort, succPort;
    public static String node_id = "";
    public static String myPort = "";

    public static final int TIMEOUT          = 1000;
    public static final int MAX_MSG_LENGTH   = 40000; //HUGE: as the whole DB data is dumped  as a string
    public static final int SERVER_PORT      = 10000;

    public static SimpleDynamoProvider singleInstance;
    public static SQLHelperClass sql;
    public final static Object lock =  new Object();
    public final static Object failureLock = new Object();

    /*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/
    public static String INSERT        = "INSERT"; //insert the data + asks for replication
    public static String REPLICATE     = "REPLICATE";  //does the replication
    public static String QUERY_LOOKUP  = "QUERY_LOOKUP";  // used by originator for both "@" and "*" query
    public static String QRY_DATA_DONE = "QRY_DATA_DONE"; // indicate the key is found in local DB; inform remote avd of the result
    public static String QUERY_GET_DATA= "QUERY_GET_DATA";// indicate the process to return local data (specified in "key", can be "@"s )
    public static String EMPTY         = "EMPTY";
    public static String DELETE_DATA   = "DELETE_DATA";
    public static String HEARTBEAT     = "HEARTBEAT"; // for send-ACK sequence of messages
    public static String RECOVER       = "RECOVER";
    public static String RECOVER_RESP  = "RECOVER_RESP";
	/*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/

    /*Helpers for query() operation*/
    public static boolean queryDone = false;
    public static String queryKey, queryValue; // stores the result sent by remote avd after querying its database
    /*Helpers for recovery operations*/
    public static HashSet<String> failedAVDList;
    public static boolean bRecover  = false;
    public static HashMap<String,String> failedMessageStorage;
    /*stores the message of type INSERT , REPLICATE which clientTask was unable to send due to avd failure
    the sender will buffer them locally in this map and send across the details when the AVD recovers*/
    /*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/

    /*%%%%%%%%%%%%%%%%%%%%% HELPER FUNCTIONS START %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/
    public String getMyPort() {
        TelephonyManager tel = (TelephonyManager)getContext().getSystemService(Context.TELEPHONY_SERVICE);

        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        return String.valueOf((Integer.parseInt(portStr) * 2));
    }


    /**
     * Checks if the hashKey belongs to the given node_id
     * node_id can be found by dynamoList
     * @param hashKey
     * @param node_id
     * @return
     */
    public boolean doLookup(String hashKey,String node_id) {
        Log.e(TAG,"doLookup, hashKey " + hashKey + " node_id " + node_id);
        String predHash = dynamoList.getPredecessor(node_id);
        Log.e(TAG,"doLookup, predHash " + predHash);
        if ( hashKey.compareTo(predHash) >0 &&  hashKey.compareTo(node_id) <= 0 ) {
            Log.e(TAG,"doLookup, return true");
            return true;
        }
        Log.e(TAG,"doLookup, return false");
        return  false;
    }
    public synchronized String getLocationOfMessage(String key) {
        Log.e(TAG,"***********To find location for key " + key);
        String hash = genHash(key);
        Log.e(TAG,"hash => " + hash + " for key " + key);
        String location = "";
        if(hash.equals("")) {
            Log.e(TAG,"Empty hash, thus empty location");
            return location;
        }
        Log.e(TAG," dynamoList.last() " + dynamoList.last());
        for(String node:dynamoList) {
            //Log.e(TAG,"To doLookup for node " + node);
            if (doLookup(hash,node)) {
                location = dynamoList.getPortFromPortHash(node);
                //Log.e(TAG,"getLocation port " + location);
            } else {
                //Log.e(TAG,"getLocation false for " + node + "...Search continues ");
            }
        }
        if (location.equalsIgnoreCase("")) {
            location = dynamoList.getPortFromPortHash(dynamoList.first());
            Log.e(TAG,"hash(key) greater / smaller than all nodes, location is first node  " + location);
        }
        Log.e(TAG,"*********Final location " + location);
        return location;
    }
    /*
    *
    */
    public static void handleFailures(String remotePort,Message message) {
        /**/
        Log.e(TAG,"To remove " + remotePort + " from aliveNode list");
        //SimpleDynamoProvider.dynamoList.removePort(remotePort);
        failedAVDList.add(remotePort);
        sql.insertFailureValues(message);
//        if (failedMessageStorage.containsKey(remotePort)) {
//            failedMessageStorage.put(remotePort,new StringBuilder(message.key).append(":").append(message.value).toString());
//        } else {
//            failedMessageStorage.put(remotePort,failedMessageStorage.get(remotePort));
//        }

    }

    /**
     * Contains all the colon-seperated key which we have to delete
     * The key is in ":" seperated format
     * @param key
     */
    public static void deleteFromFailureTable(String key) {
        Log.e(TAG,"deleteFromFailureTable for key " + key);
        String[] keyArr = key.split(":");
        for(int i=0; i < keyArr.length; i++) {
            Log.e(TAG,"deleteFromFailureTable KEY " + keyArr[i]);
            //sql.deleteDataFromFailureTable(keyArr[i]);
        }
    }

    /**
     *
     */
    private void createServerSocket() {
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket " + e.getMessage());
            return;
        }

    }
    public long insertIntoDatabase(ContentValues cv) {
        if (sql.insertValues(cv) == -1) {
            Log.e(TAG, "Insertion into db failed for values :" + cv.toString());
            return -1;
        }
        Log.e(TAG,"Database insertion success for values " + cv.toString());
//        synchronized (this ) {
            SimpleDynamoActivity.getInstance().setText(cv.toString());
//        }
        return 0;
    }
    /**
     * Send generic message to remote port set in message.remotePort
     * @param message
     */
    public static void sendMessageToRemotePort(Message message) {

        if (failedAVDList.contains(message.remotePort)) {
            if (message.messageType.equalsIgnoreCase(INSERT) || message.messageType.equalsIgnoreCase(REPLICATE)) {
                Log.e(TAG,"sendMessageToRemotePort; port has failed " + message.remotePort + ".. log to failDB instead");
                sql.insertFailureValues(message);
                return;
            }
        }
        Log.e(TAG,"To send messsage to " + message.remotePort + " messageType " + message.messageType);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
    }
    /**
     * Used by contentProvider to wait for response for various requests
     */
    private void waitForResponse() {
        while(!queryDone) {
            synchronized (lock) {
                //Log.e("waitForResponse","waiting for response ");
                try {
                    lock.wait(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
            }
        }
//        synchronized (lock) {
            queryDone = false;
//        }
    }

    /**
     *
     * @param remotePort
     * @return
     */
    public Cursor returnFailureTableDataForRemote(String remotePort) {
        Cursor c;

        c = sql.getFailureData(null,"port=?",new String[]{remotePort},null);
        return c;
    }

    /**
     * Return local data, either full database or specific key (dependent on parameter)
     * @param selection
     * @return
     */
    public Cursor returnLocalData(String selection) {
        Cursor c;
        if (selection == null || selection.equalsIgnoreCase(SimpleDynamoActivity.GDumpSelection))
            c = sql.getData(null,null,null,null);
        else
            c=  sql.getData(null, "key=?",new String[]{selection}, null);
        return c;
    }


    /*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/

	@Override
	public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.e(TAG,"selection " + selection);
        int rowsAffected = 0;
        if (selection.equalsIgnoreCase(SimpleDynamoActivity.LDumpSelection)) {
            //delete all rows from local database
            Log.e(TAG,"LDump parameter. delete all rows from local DB");
            rowsAffected = sql.deleteDataFromTable(null);
            return rowsAffected;
        }
        String key ;
        if (selection.equalsIgnoreCase(SimpleDynamoActivity.GDumpSelection))
            key = null;
        else
            key = selection;
        rowsAffected    = sql.deleteDataFromTable(key);
        Message message = new Message(selection,"dummy",DELETE_DATA,myPort,"dummy","-1");
        for(String node: dynamoList) {

            String remotePort = dynamoList.getPortFromPortHash(node);
            if (!remotePort.equalsIgnoreCase(myPort)) {
                message.remotePort = remotePort;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message);
            }
        }
		return rowsAffected;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
        String key  = values.get(SimpleDynamoActivity.KEY_FIELD).toString();
        String value= values.get(SimpleDynamoActivity.VALUE_FIELD).toString();
        Log.e(TAG,"INSERT--->" +  "key " + key + " : " + value);
        String location = getLocationOfMessage(key);
        Log.e(TAG,"Found location " + location);
        if (location.equalsIgnoreCase(myPort)) {
            Log.e(TAG,"Insert into my db");
            insertIntoDatabase(values);
        }
        Log.e(TAG,"To send message "  + key + ":" + value + " to location " + location);
        Message message = new Message(key, value,INSERT,myPort,location,"-1");
        sendMessageToRemotePort(message);
        //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message);
		return uri;
	}
    private void printTreeSet(TreeSet<String> treeSet) {
        Log.e(TAG,"printTreeSet starts");
        for(String node: treeSet) {
            Log.e(TAG,node);
        }
        Log.e(TAG,"printTreeSet ends");
    }
    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        Cursor c = null;
        Log.e(TAG,"query is "  + selection);
        if (selection.equalsIgnoreCase(SimpleDynamoActivity.LDumpSelection)) { //Local dump
            Log.e(TAG,"Local dump data");
            c = returnLocalData(null);
        }
        else if (selection.equalsIgnoreCase(SimpleDynamoActivity.GDumpSelection)) { //global dump
            Log.e(TAG,"GDump selection parameter , ask for all the data");
            MatrixCursor matrixCursor = new MatrixCursor(new String[]{SimpleDynamoActivity.KEY_FIELD,SimpleDynamoActivity.VALUE_FIELD});
            Message message = new Message(selection,"dummy",QUERY_GET_DATA,myPort,"dummy","-1");
            for(String node:dynamoList) {
                String remotePort = dynamoList.getPortFromPortHash(node);
                message.remotePort = remotePort;
                Log.e(TAG,"GDump parameter for node " + remotePort);
                new ClientTask().execute(message);
                waitForResponse();
                if (!SimpleDynamoProvider.queryKey.equalsIgnoreCase(SimpleDynamoProvider.EMPTY)) {
                    String[] keyArr = SimpleDynamoProvider.queryKey.split(":");
                    String[] valArr = SimpleDynamoProvider.queryValue.split(":");

                    for(int i=0; i < keyArr.length; i++) {
                        String[] results  = new String[]{keyArr[i],valArr[i]};
                        matrixCursor.addRow(results);
                    }
                } else {
                    Log.e(TAG,"No data in " + message.remotePort);
                }

            }
            return matrixCursor;
        }
        else {
            /*specific key*/
            c = sql.getData(null, "key=?",new String[]{selection}, null);
            if ((c== null) || (c.getCount() == 0)) { //find the actual location
                Log.e(TAG,"Not found in local DB ! To find actual location for message "  + selection);
                String location = getLocationOfMessage(selection);
                Log.e(TAG,"Actual location is " + location);
                Message message = new Message(selection,"dummy",QUERY_GET_DATA,myPort,location,"-1");
                new ClientTask().execute(message);
                waitForResponse();
                synchronized (lock) {
                    SimpleDynamoActivity.getInstance().setText("\n*** QUERY RESULTS **** =" + SimpleDynamoProvider.queryKey + " :: " +
                            SimpleDynamoProvider.queryValue + "\n");
                }
                String[] results = new String[]{SimpleDynamoProvider.queryKey, SimpleDynamoProvider.queryValue};
                MatrixCursor matrixCursor = new MatrixCursor(new String[]{SimpleDynamoActivity.KEY_FIELD,SimpleDynamoActivity.VALUE_FIELD});
                matrixCursor.addRow(results);
                return matrixCursor;

            } else {
                Log.e(TAG,"Found in local DB itself !");
            }
        }
        return c;
    }
    public void checkIfNodeIsRecovered() {
        Log.e(TAG,"checkIfNodeIsRecovered");
        SharedPreferences sharedPreferences = this.getContext().getSharedPreferences("startupPref",Context.MODE_PRIVATE);
        Log.e(TAG,"pref check " + sharedPreferences.getBoolean("initialStartup",true));
        if (sharedPreferences.getBoolean("initialStartup",true)) {
            sharedPreferences.edit().putBoolean("initialStartup",false).commit();
        } else
            bRecover = true;
    }
    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }
    /*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/
	@Override
	public boolean onCreate() {
        sql = SQLHelperClass.getInstance(getContext());
        myPort = getMyPort();
        createServerSocket();
        if (singleInstance == null) {
            singleInstance = this;
        }
        node_id = genHash(String.valueOf(Integer.valueOf(myPort) / 2));
        failedMessageStorage = new HashMap<String, String>();
        failedAVDList = new HashSet<String>();
        //new ClientTask().executeOnExecutor()
        dynamoList = new DynamoList<String>();
        String hash = "";
        checkIfNodeIsRecovered();
        for(int i=0; i < AVD_LIST.length; i++) {
            hash = genHash(String.valueOf(AVD_LIST[i]));
            Log.e(TAG,"port " + String.valueOf(AVD_LIST[i]) + " hash " + hash);
            dynamoList.add(genHash(String.valueOf(AVD_LIST[i])));
            Message message = new Message("dummy","dummy",RECOVER,myPort,String.valueOf(REMOTE_PORT[i]),"-1");
            if (bRecover) {
                Log.e(TAG,"Sending recovery message to " + REMOTE_PORT[i]);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message);
            }
        }
        //bRecover = false;
        /**
         * We save the startUp state to the shared preferences
         * Snippet referred (modified for my application) from
         * "http://stackoverflow.com/questions/5950043/how-to-use-getsharedpreferences-in-android"
         */


        printTreeSet(dynamoList);
        /*To check if the node has recovered, it must send "RECOVER" message type to all the dynamo nodes (be safe always)
        * NOTE : Sending to all nodes is required as failed INSERT messages are buffered in the sender side, and the sender could be anyone in the right
        * Of course, REPLICATE messages will be only among the previous 2 predecessors, but who cares
        */


		return true;
	}


    public static String genHash(String input)  {
        try
        {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] sha1Hash = sha1.digest(input.getBytes());
            Formatter formatter = new Formatter();
            for (byte b : sha1Hash) {
                formatter.format("%02x", b);
            }
            return formatter.toString();
        } catch(NoSuchAlgorithmException e) {

            e.printStackTrace();
            return "";
        }

    }

    /**
     * Singleton instance pattern
     * @return
     */
    public static SimpleDynamoProvider getInstance() {
        return singleInstance;
    }


}
