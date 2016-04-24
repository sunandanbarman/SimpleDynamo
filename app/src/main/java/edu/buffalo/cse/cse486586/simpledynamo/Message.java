package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

/**
 * Similiar to GroupMessenger2, this class creates a message with fields
 * Key , Value, hash(Key), messageType, originPort, remotePort, predPort , succPort
 * 1. msgType 2. originPort 3. hash(key) 4. key 5. value 6. messageType
 * For sending and receiving, we use the deConstruct and reConstruct procedures
 * Created by sunandan on 4/15/16.
 */
public class Message {
    public String key,value, messageType;
    public String replicationCount;
    public String originPort, remotePort;
//    public String predPort, succPort; //use only in case of NODE_ADDED message

    Message() {

    }
    Message(Message m1) {
        this.key  = m1.key;
        this.value= m1.value;
        this.messageType = m1.messageType;
        this.originPort  = m1.originPort;
        this.remotePort  = m1.remotePort;
        this.replicationCount = m1.replicationCount;
    }
    Message(String text) {
        this.reconstructMessage(text);
    }
//    Message(String key, String value, String hashKey, String messageType, String originPort,String remotePort, String predPort, String succPort) {
//        this.key  = key;
//        this.value= value;
//        this.hashKey = hashKey;
//        this.messageType = messageType;
//        this.originPort  = originPort;
//        this.remotePort  = remotePort;
//        this.predPort    = predPort;
//        this.succPort    = succPort;
//    }
    Message(String key, String value, String messageType, String originPort,String remotePort, String replicationCount) {
        this.key  = key;
        this.value= value;
        //this.hashKey = hashKey;
        this.messageType = messageType;
        this.originPort  = originPort;
        this.remotePort  = remotePort;
        this.replicationCount = replicationCount;
//        this.predPort    = predPort;
//        this.succPort    = succPort;
    }

    /** split the incomingMessage and fill in the details in "this" object**/
    public void reconstructMessage(String incomingMessage) {
        Log.e("reconstructMessage","incomingMessage "  + incomingMessage);
        String[] msgs = incomingMessage.split(";");
        try {
            this.key        = msgs[0];
            this.value      = msgs[1];
            this.messageType= msgs[2];
            this.originPort = msgs[3];
            this.remotePort = msgs[4];
            this.replicationCount = msgs[5];
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    /** Use only in client-server communication**/
    public String deconstructMessage() {
        return  this.key           + ";"
                + this.value       + ";"
                + this.messageType + ";"
                + this.originPort  + ";"
                + this.remotePort  + ";"
                + this.replicationCount  + ";";
    }
    /**
     *
     * @return
     */
    public String toString() {
        return  "key   :"       + this.key     + ";"
                + "value:"     + this.value  + ";"
                + "messageType:"      + this.messageType + ";"
                + "originPort:"       + this.originPort  + ";"
                + "remotePort:"       + this.remotePort  + ";"
                + "replicationCount:" + this.replicationCount  + ";";
    }

}
