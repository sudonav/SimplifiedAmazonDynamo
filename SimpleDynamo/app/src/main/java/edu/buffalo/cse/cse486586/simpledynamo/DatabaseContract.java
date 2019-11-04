package edu.buffalo.cse.cse486586.simpledynamo;

import android.provider.BaseColumns;

    /* References:
    * https://developer.android.com/training/data-storage/sqlite#java
    * Created the following:
    * 1) DatabaseContract as per the android documentation to name the database, table and its columns
    * 2) SQL queries to create and destroy the created table
    */

public final class DatabaseContract {

    public static final int DATABASE_VERSION = 1;
    public static final String DATABASE_NAME = "messenger.db";

    private DatabaseContract() {
    }

    public static class Messenger implements BaseColumns {
        public static final String TABLE_NAME = "message";
        public static final String COLUMN_NAME_MESSAGE_KEY = "key";
        public static final String COLUMN_NAME_MESSAGE_VALUE = "value";
//        public static final String COLUMN_NAME_MESSAGE_NODE = "node";

        public static final String SQL_CREATE_ENTRIES =
                "CREATE TABLE " + TABLE_NAME + " (" +
                        COLUMN_NAME_MESSAGE_KEY + " TEXT PRIMARY KEY," +
                        COLUMN_NAME_MESSAGE_VALUE + " TEXT)";
//                        +
//                        COLUMN_NAME_MESSAGE_NODE +" TEXT)";

        public static final String SQL_DELETE_ENTRIES =
                "DROP TABLE IF EXISTS " + TABLE_NAME;

        public static final String[] PROJECTION = new String[]{COLUMN_NAME_MESSAGE_KEY, COLUMN_NAME_MESSAGE_VALUE};

    }
}
