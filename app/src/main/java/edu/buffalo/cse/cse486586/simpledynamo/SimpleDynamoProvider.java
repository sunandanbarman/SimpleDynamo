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
    public static final int MAX_MSG_LENGTH   = 4096; //HUGE: as the whole DB data is dumped  as a string
    public static final int SERVER_PORT      = 10000;

    /*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/

    /*%%%%%%%%%%%%%%%%%%%%% HELPER FUNCTIONS START %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/
    public String getMyPort() {
        TelephonyManager tel = (TelephonyManager)getContext().getSystemService(Context.TELEPHONY_SERVICE);

        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        return String.valueOf((Integer.parseInt(portStr) * 2));
    }

    private static boolean isOnlyNodeInRing() {
        return predHash.equalsIgnoreCase("") && succHash.equalsIgnoreCase("");
    }

    /**
     * Checks if the hashKey belongs to the given node_id
     * node_id can be found by dynamoList
     * @param hashKey
     * @param node_id
     * @return
     */
    public boolean doLookup(String hashKey,String node_id) {
        String predHash = dynamoList.getPredecessor(node_id);
        if ( hashKey.compareTo(predHash) >0 &&  hashKey.compareTo(node_id) <= 0 ) {
            return true;
        }
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
        String hash = genHash(key);
        String location = "";
        if(hash.equals("")) {
            return location;
        }
        for(String node:dynamoList) {
            if (doLookup(hash,node)) {
                location = dynamoList.getPortFromPortHash(node);
            }
        }

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
    public static long insertIntoDatabase(ContentValues cv) {
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


    /*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
        String port = "";
        String key  = values.get(SimpleDynamoActivity.KEY_FIELD).toString();
        String value= values.get(SimpleDynamoActivity.VALUE_FIELD).toString();
        //String hashKey = "";
        Log.e(TAG,"INSERT--->" +
                "key " + key + " : " + value);
        //hashKey = genHash(key);
        String location = getLocationOfMessage(key);
        Log.e(TAG,"Found location " + location);
        if (location.equals(myPort)) {
            if (insertIntoDatabase(values) == -1) {
                return null;
            } else {
                return uri;
            }
        }
        Log.e(TAG,"To send message "  + key + ":" + value + " to location " + location);
        Message message = new Message(key, value,SimpleDynamoActivity.INSERT,myPort,location);
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
		// TODO Auto-generated method stub
		return null;
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

}
