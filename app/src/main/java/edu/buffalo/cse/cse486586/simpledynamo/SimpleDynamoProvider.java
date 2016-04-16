package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.net.ServerSocket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.TreeMap;
import java.util.TreeSet;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

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
    public final static Object lock =  new Object();

    /*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/
    public static String INSERT        = "INSERT"; //insert the data + asks for replication
    public static String REPLICATE     = "REPLICATE";  //does the replication
    public static String QUERY_LOOKUP  = "QUERY_LOOKUP";  // used by originator for both "@" and "*" query
    public static String QRY_DATA_DONE = "QRY_DATA_DONE"; // indicate the key is found in local DB; inform remote avd of the result
    public static String QUERY_GET_DATA= "QUERY_GET_DATA";// indicate the process to return local data (specified in "key", can be "@"s )
    public static String EMPTY         = "EMPTY";
    public static String DELETE_DATA   = "DELETE_DATA";
	/*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/

    /*Helpers for query() operation*/
    public static boolean queryDone = false;
    public static String queryKey, queryValue; // stores the result sent by remote avd after querying its database

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
//    public static boolean doLookup(String hashKey) {
//        if (isOnlyNodeInRing()) {
//            Log.e(TAG,"No predecessor and successor, hence inserting within own provider");
//            return true;
//        }
//        Log.e(TAG,"predHash " + predHash + " node_id " + node_id);
//        Log.e(TAG," hashCompare :" + hashKey.compareTo(predHash));
//        Log.e(TAG," hashCompare :" + hashKey.compareTo(node_id));
//
//        if (hashKey.compareTo(predHash) > 0 && hashKey.compareTo(node_id) <= 0) {
//            return true;
//        }
//        if (hashKey.compareTo(node_id)> 0 && hashKey.compareTo(predHash) > 0 && node_id.compareTo(predHash) < 0) { //first node, hashkey greater than all nodes
//            return  true;
//        }
//        if (hashKey.compareTo(node_id) < 0 && hashKey.compareTo(predHash) <0 && node_id.compareTo(predHash) < 0 ) { //first node, hashKey lesser than all nodes
//            return true;
//        }
//        return false;
//    }
    public String getLocationOfMessage(String key) {
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
                //Log.e(TAG,"getLocation true for " + node);
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
        if (SimpleDynamoActivity.sql.insertValues(cv) == -1) {
            Log.e(TAG, "Insertion into db failed for values :" + cv.toString());
            return -1;
        }
        Log.e(TAG,"Database insertion success for values " + cv.toString());
        SimpleDynamoActivity.getInstance().setText(cv.toString());
        return 0;
    }
    /**
     * Send generic message to remote port set in message.remotePort
     * @param message
     */
    public static void sendMessageToRemotePort(Message message) {
        Log.e(TAG,"To send messsage to " + message.remotePort + " messageType " + message.messageType);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
    }
    /**
     * Used by contentProvider to wait for response for various requests
     */
    private void waitForResponse() {
        while(!queryDone) {
            synchronized (lock) {
                try {
                    lock.wait(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
            }
        }
        queryDone = false;
    }
    /**
     * Return local data, either full database or specific key (dependent on parameter)
     * @param selection
     * @return
     */
    public Cursor returnLocalData(String selection) {
        Cursor c;
        if (selection == null || selection.equalsIgnoreCase(SimpleDynamoActivity.GDumpSelection))
            c = SimpleDynamoActivity.sql.getData(null,null,null,null);
        else
            c=  SimpleDynamoActivity.sql.getData(null, "key=?",new String[]{selection}, null);
        return c;
    }


    /*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.e(TAG,"selection " + selection);
        int rowsAffected = 0;
        if (selection.equalsIgnoreCase(SimpleDynamoActivity.LDumpSelection)) {
            //delete all rows from local database
            Log.e(TAG,"LDump parameter. delete all rows from local DB");
            rowsAffected = SimpleDynamoActivity.sql.deleteDataFromTable(null);
            return rowsAffected;
        }
        String key ;
        if (selection.equalsIgnoreCase(SimpleDynamoActivity.GDumpSelection))
            key = null;
        else
            key = selection;
        rowsAffected    = SimpleDynamoActivity.sql.deleteDataFromTable(key);
        Message message = new Message(selection,"dummy",DELETE_DATA,myPort,"dummy");
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
        //String port = "";
        String key  = values.get(SimpleDynamoActivity.KEY_FIELD).toString();
        String value= values.get(SimpleDynamoActivity.VALUE_FIELD).toString();
        //String hashKey = "";
        Log.e(TAG,"INSERT--->" +  "key " + key + " : " + value);
        //hashKey = genHash(key);
        String location = getLocationOfMessage(key);
        Log.e(TAG,"Found location " + location);
//        if (location.equals(myPort)) {
//            Log.e(TAG,"Insert into my db");
//            if (insertIntoDatabase(values) == -1) {
//                return null;
//            } else {
//                return uri;
//            }
//        }
        Log.e(TAG,"To send message "  + key + ":" + value + " to location " + location);
        Message message = new Message(key, value,INSERT,myPort,location);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message);
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
	public boolean onCreate() {
        myPort = getMyPort();
        createServerSocket();
        if (singleInstance == null) {
            singleInstance = this;
        }
        node_id = genHash(String.valueOf(Integer.valueOf(myPort) / 2));

        dynamoList = new DynamoList<String>();
        String hash = "";
        for(int i=0; i < AVD_LIST.length; i++) {
            hash = genHash(String.valueOf(AVD_LIST[i]));
            Log.e(TAG,"port " + String.valueOf(AVD_LIST[i]) + " hash " + hash);
            dynamoList.add(genHash(String.valueOf(AVD_LIST[i])));
        }
        printTreeSet(dynamoList);

		return true;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
        Cursor c = null;
        Log.e(TAG,"query is "  + selection);
//        if ((  selection.equalsIgnoreCase(SimpleDynamoActivity.LDumpSelection) || selection.equalsIgnoreCase(SimpleDynamoActivity.GDumpSelection))) {
//            selection = SimpleDynamoActivity.LDumpSelection;
//        }
        if (selection.equalsIgnoreCase(SimpleDynamoActivity.LDumpSelection)) { //Local dump
            Log.e(TAG,"Local dump data");
            c = returnLocalData(null);
            //return c;
        }
        else if (selection.equalsIgnoreCase(SimpleDynamoActivity.GDumpSelection)) { //global dump
            Log.e(TAG,"GDump selection parameter , ask for all the data");
            MatrixCursor matrixCursor = new MatrixCursor(new String[]{SimpleDynamoActivity.KEY_FIELD,SimpleDynamoActivity.VALUE_FIELD});
            Message message = new Message(selection,"dummy",QUERY_GET_DATA,myPort,"dummy");
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
            c = SimpleDynamoActivity.sql.getData(null, "key=?",new String[]{selection}, null);
            if ((c== null) || (c.getCount() == 0)) { //find the actual location
                Log.e(TAG,"Not found in local DB ! To find actual location");
                String location = getLocationOfMessage(selection);
                Log.e(TAG,"Actual location is " + location);
                Message message = new Message(selection,"dummy",QUERY_GET_DATA,myPort,location);
                new ClientTask().execute(message);
                waitForResponse();
                SimpleDynamoActivity.getInstance().setText("\n*** QUERY RESULTS **** ="  + SimpleDynamoProvider.queryKey + " :: " +
                                                            SimpleDynamoProvider.queryValue + "\n");
                String[] results = new String[]{SimpleDynamoProvider.queryKey, SimpleDynamoProvider.queryValue};
                MatrixCursor matrixCursor = new MatrixCursor(new String[]{SimpleDynamoActivity.KEY_FIELD,SimpleDynamoActivity.VALUE_FIELD});
                matrixCursor.addRow(results);
                return matrixCursor;

            } else {
                Log.e(TAG,"Found in local DB itself !");
            }
        }
        //String location = getLocationOfMessage(key);

		return c;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
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
