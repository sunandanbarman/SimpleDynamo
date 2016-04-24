package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.util.HashMap;
import java.util.TreeSet;

/**
 * Created by sunandan on 3/24/16.
 */

/**
 * DynamoList stores the hashes of ports (11112,11116 etc..)
 * getPredecessor takes the hash(port) and returns the actual predecessor port (e.g. 11112, etc..)
 * getSuccessor does the same; only thing is that it returns a successor
 * @param <String>
 */
public class DynamoList<String> extends TreeSet<String> {
    String myPort;
    private final static Object dynLock=  new Object();
    private static boolean dynLockFlag =  false;
    private HashMap<String,String> hashWithPortMap;
    DynamoList() {
        hashWithPortMap = new HashMap<String, String>();
        for(int port:SimpleDynamoProvider.REMOTE_PORT) {
            hashWithPortMap.put(
                        (String)(SimpleDynamoProvider.genHash(java.lang.String.valueOf(port/2))),
                        (String)java.lang.String.valueOf(port));
        }
    }

    private void waitForResponse() {
        while (dynLockFlag) {
            synchronized (dynLock) {
                try {
                    Log.e("DyListWaitForResponse","wait for response");
                    dynLock.wait(50);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
            }
        }
        dynLockFlag = false;
    }
    /**
     * Adds the hash(port) to the chordList
     * @param obj
     * @return
     */
    @Override
    public boolean add(String obj) {
        return super.add(obj);
    }

    /**
     * Failure handling method
     * @param obj
     * @return
     */
    public boolean removePort(String obj) {
        dynLockFlag = true;
        int avdID   = Integer.valueOf(obj.toString()) / 2;
        String hashPort = (String)SimpleDynamoProvider.genHash(java.lang.String.valueOf(avdID));
        Log.e("removePort","to remove " + obj.toString());
        removeFailedPortFromList(hashPort);
        return super.remove(hashPort);
    }

    public String getPortFromPortHash(String portHash) {
        //waitForResponse();
        return hashWithPortMap.get(portHash);

    }
    /**
     * Returns the Predecessor of the hash(port)
     * @param portHash
     * @return
     */
    public String getPredecessor(String portHash) {
        //waitForResponse();
        if (this.contains(portHash)) {

            if (this.first().equals(portHash)) {
                return this.last();
            }
            if (this.size() == 2) {
                if (this.last().equals(portHash)) {
                    return this.first();
                }
            }
            return this.lower(portHash);
        }
        return null;
    }
    /**
     * Returns the successor of the hash(port)
     * @param portHash
     * @return
     */
    public String getSuccessor(String portHash) {
        //waitForResponse();
        if (this.contains(portHash)) {
            if (this.last().equals(portHash)) {
                return this.first();
            }
            return this.higher(portHash);
        }
        return null;
    }

    /**
     * Remove the failed remote port from port map
     * @param portHash
     */
    public void removeFailedPortFromList(String portHash) {
        Log.e("removeFailedPortFromLis"," remotePort " + portHash.toString());
        synchronized (dynLock) {
            Log.e("removeFailedPortFromLis","1.1 remotePort " + portHash.toString());
            hashWithPortMap.remove(portHash);
            dynLockFlag = false;
            dynLock.notifyAll();
            Log.e("removeFailedPortFromLis","done for " + portHash.toString());
        }

    }
}
