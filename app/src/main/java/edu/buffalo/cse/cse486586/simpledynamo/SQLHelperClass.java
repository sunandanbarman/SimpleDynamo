package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by sunandan on 24 Feb 16
 * Database code referred from :
 * http://developer.android.com/training/basics/data-storage/databases.html
 */

//class DatabaseManager {
//
//    private AtomicInteger mOpenCounter = new AtomicInteger();
//
//    private static DatabaseManager instance;
//    private static SQLiteOpenHelper mDatabaseHelper;
//    private SQLiteDatabase mDatabase;
//
//    public static synchronized void initializeInstance(SQLiteOpenHelper helper) {
//        if (instance == null) {
//            instance = new DatabaseManager();
//            mDatabaseHelper = helper;
//        }
//    }
//
//    public static synchronized DatabaseManager getInstance() {
//        if (instance == null) {
//            throw new IllegalStateException(DatabaseManager.class.getSimpleName() +
//                    " is not initialized, call initializeInstance(..) method first.");
//        }
//
//        return instance;
//    }
//
//    public synchronized SQLiteDatabase openDatabase() {
//        if(mOpenCounter.incrementAndGet() == 1) {
//            // Opening new database
//            mDatabase = mDatabaseHelper.getWritableDatabase();
//        }
//        return mDatabase;
//    }
//
//    public synchronized void closeDatabase() {
//        if(mOpenCounter.decrementAndGet() == 0) {
//            // Closing database
//            mDatabase.close();
//
//        }
//    }
//}

class SQLHelperClass extends  SQLiteOpenHelper{
    private String TAG = SQLHelperClass.class.getName();
    private static String DB_TABLE = "Data";
    private static String DB_NAME  = "SimpleDynamo.db";
    public static int DB_VERSION = 1;

    private static SQLHelperClass instance_;
    private static SQLiteDatabase db = null;
    /***
     * column names
     */
    private static String COLUMN_KEY = "key";
    private static String COLUMN_VAL = "value";
    private static String TEXT_TYPE  = " TEXT ";

    private static final String SQL_CREATE_TABLE =
            "CREATE TABLE " + DB_TABLE + " ( " + COLUMN_KEY + " STRING PRIMARY KEY,"
                    + COLUMN_VAL + TEXT_TYPE + " )"
            ;
    /**
     * Create a helper object to create, open, and/or manage a database.
     * This method always returns very quickly.  The database is not actually
     * created or opened until one of {@link #getWritableDatabase} or
     * {@link #getReadableDatabase} is called.
     *
     * @param context to use to open or create the database
     *
     * Hide away the constructor to use a single instance of SQLite DB
     *
     * @param context
     */
    private SQLHelperClass(Context context) {
        super(context, DB_NAME, null, DB_VERSION);
        Log.e(TAG, "Class created");
        db = null;
    }

    /**
     * Singleton instance returned in a thread-safe manner
     * @param context
     * @return
     */
    public static synchronized SQLHelperClass getInstance(Context context) {

        if (instance_ == null) {
            db        = null;
            instance_ = new SQLHelperClass(context.getApplicationContext());
        }
        return instance_;
    }

    /**
     * This method DOES NOT get called if actual DB exists ! Don't rely on this to init your db instance
     * @param db
     */
    @Override
    public void onCreate(SQLiteDatabase db) {

        db.execSQL(SQL_CREATE_TABLE);
        if (db == null) {
            Log.e("ONCREATE","HOW Can this happen ?");
        }
        if (SQLHelperClass.db == null) {
            SQLHelperClass.db = db;
            Log.e("ONCREATE","DBPATH " + SQLHelperClass.db.getPath());

        } else {
            Log.e("ONCREATE","static db path :" + SQLHelperClass.db.getPath());
        }
        SQLHelperClass.db = db;

        Log.e(TAG,"Database created :" + SQLHelperClass.db.getPath());
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

    }

    /**
     * Use this method to insert the values into DB
     * @param cv
     * @return
     */
    public synchronized long insertValues(ContentValues cv) {
        // Gets the data repository in write mode
        //SQLiteDatabase db = this.getWritableDatabase();

        // Insert the new row, returning the primary key value of the new row
        // insertWithOnConflict takes care to replace the row in case value already exists
        // ensuring key=value is always up-to date
        long newRowID = -1;
        try {
            if (SQLHelperClass.db == null) {
                Log.e("INSERTVALUES", "WTF !! DID not init DB");
                SQLHelperClass.db = this.getWritableDatabase();
            }
            newRowID = SQLHelperClass.db.insertWithOnConflict(DB_TABLE, null, cv, SQLiteDatabase.CONFLICT_REPLACE);
        } catch(Exception ex) {
            //Log.e("INSERTVALUES",ex.)
            ex.printStackTrace();
        }
        return newRowID;
    }

    public synchronized Cursor getData(String[] projection, String selection, String[] selectionArgs,
                          String sortOrder) {

        if (SQLHelperClass.db == null) {
            SQLHelperClass.db = this.getReadableDatabase();
        }
        Cursor c = null;
        try {
            c = SQLHelperClass.db.query(DB_TABLE, projection, selection, selectionArgs, null, null, sortOrder);
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return c;
    }

    /**
     * Call this method when a specific row entry has to be deleted
     * Pass null in parameter to delete entire table
     * @param key
     */
    public synchronized int deleteDataFromTable(String key) {
        int rowsAffected;
        if (SQLHelperClass.db == null) {
            SQLHelperClass.db = this.getWritableDatabase();
        }

        if (key == null) {
            rowsAffected = SQLHelperClass.db.delete(DB_TABLE,null,null);
        } else {
            rowsAffected = SQLHelperClass.db.delete(DB_TABLE,"key=?",new String[]{key});
        }

        return rowsAffected;
    }

}

