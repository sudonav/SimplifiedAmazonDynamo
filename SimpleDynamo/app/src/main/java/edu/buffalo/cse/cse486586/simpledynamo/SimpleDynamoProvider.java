package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.provider.Telephony;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;

	static final String URL = "content://edu.buffalo.cse.cse486586.simpledynamo.provider";
	static final Uri CONTENT_URL = Uri.parse(URL);

	private SQLiteDatabase messageDB;
	private String myPort;
	private static int count = 0;

	private static final int replicationFactor = 3;

	private static final String remoteNode_5554 = "5554";
	private static final String remoteNode_5556 = "5556";
	private static final String remoteNode_5558 = "5558";
	private static final String remoteNode_5560 = "5560";
	private static final String remoteNode_5562 = "5562";

	private static String[] availableNodes = {remoteNode_5562, remoteNode_5554, remoteNode_5556, remoteNode_5558, remoteNode_5560};
	private static List<String> dynamoRing;
	private static TreeMap<String, String> hashedNodeList = new TreeMap<String, String>();
	private static ConcurrentHashMap<String, List<String>> replicaMap;
	private static ConcurrentHashMap<String, List<String>> predecessorMap;
	private static LinkedList<String> holdBackQueue = new LinkedList<String>();

	static final String statusSuccess = "SUCCESS";
	static final String statusFailure = "FAILURE";

	static final String msgTypeInsertReplicaReq = "INSERT_REPLICA_REQUEST";
	static final String msgTypeInsertReplicaRes = "INSERT_REPLICA_RESPONSE";

	static final String msgTypeDeleteReplicaReq = "DELETE_REPLICA_REQUEST";
	static final String msgTypeDeleteRes = "DELETE_RESPONSE";

	static final String msgTypeQueryReq = "QUERY_REQUEST";
	static final String msgTypeQueryRes = "QUERY_RESPONSE";

	static final String msgTypeQueryAllReq = "QUERY_ALL_REQUEST";
	static final String msgTypeQueryAllRes = "QUERY_ALL_RESPONSE";

	static final String msgTypeDeleteAllReq = "DELETE_ALL_REQUEST";
	static final String msgTypeDeleteAllRes = "DELETE_ALL_RESPONSE";

	static final String msgTypeRecoveryReq = "RECOVERY_REQUEST";
	static final String msgTypeRecoveryRes = "RECOVERY_RESPONSE";

	public static MatrixCursor queryAllResult;
	public static MatrixCursor queryResult;

	private static final ReentrantLock insertReplicaLock = new ReentrantLock();

	private static final ReentrantLock deleteLock = new ReentrantLock();
	private static final ReentrantLock deleteAllLock = new ReentrantLock();

	private static final ReentrantLock queryLock = new ReentrantLock();
	private static final ReentrantLock queryAllLock = new ReentrantLock();

	private static int deleteAllCount = 0;
	private static int deleteCount = 0;

	public static boolean isRecoveryInProgress = false;
	public static int recoveryResponseCount = 0;

	@Override
	public boolean onCreate() {
		/* Setting up the port number */
		Log.e(TAG, "On Create works!");
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr)));

		DatabaseHelper databaseHelper = DatabaseHelper.getInstance(getContext());
		messageDB = databaseHelper.getWritableDatabase();

		try {
			dynamoRing = initializeDynamoRing();
			assignReplicaNodes();
			assignPredecessorNodes();

			Log.e(TAG, dynamoRing.toString());
			Log.e(TAG, replicaMap.toString());
			Log.e(TAG, predecessorMap.toString());
		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "No such algorithm exception");
			e.printStackTrace();
		}
		
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			e.printStackTrace();
		}

		return messageDB != null;
	}

	private ArrayList<String> initializeDynamoRing() throws NoSuchAlgorithmException {
		List<String> availableNodesList = Arrays.asList(availableNodes);
		for(String everyNode : availableNodesList) {
			hashedNodeList.put(this.genHash(everyNode), everyNode);
		}
		return new ArrayList<String>(hashedNodeList.values());
	}

	private void assignReplicaNodes() {
		if(dynamoRing != null && dynamoRing.size() == 5) {
			replicaMap = new ConcurrentHashMap<String, List<String>>();
			for(int i = 0; i < dynamoRing.size(); i++) {
				ArrayList<String> replicaList = new ArrayList<String>();
                int j = i;
				replicaList.add(dynamoRing.get(j));
				for(int k = 0; k < replicationFactor - 1; k++) {
					if(j >= dynamoRing.size() - 1) {
						j = -1;
						replicaList.add(dynamoRing.get(j + 1));
					} else {
						replicaList.add(dynamoRing.get(j + 1));
					}
					j++;
				}
				replicaMap.put(dynamoRing.get(i), replicaList);
			}
		}
	}

	private void assignPredecessorNodes() {
		if(dynamoRing != null && dynamoRing.size() == 5) {
			predecessorMap = new ConcurrentHashMap<String, List<String>>();
			for(int i = dynamoRing.size() - 1; i >= 0 ; i--) {
				ArrayList<String> predecessorList = new ArrayList<String>();
				int j = i;
				predecessorList.add(dynamoRing.get(j));
				for(int k = 0; k < replicationFactor - 1; k++) {
					if(j == 0) {
						j = 5;
						predecessorList.add(dynamoRing.get(j - 1));
					} else {
						predecessorList.add(dynamoRing.get(j - 1));
					}
					j--;
				}
				predecessorMap.put(dynamoRing.get(i), predecessorList);
			}
		}
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		DatabaseHelper databaseHelper = DatabaseHelper.getInstance(getContext());
		messageDB = databaseHelper.getWritableDatabase();
		String _selection = DatabaseContract.Messenger.COLUMN_NAME_MESSAGE_KEY + "='" + selection + "'";
		int count = messageDB.update(DatabaseContract.Messenger.TABLE_NAME,values,_selection,selectionArgs);
		Log.v("update", selection);
		return count;
	}

	private class ClientTask extends AsyncTask<String, Void, String> {

		@Override
		protected String doInBackground(String... msgs) {
			String status = null;
			Socket socket = null;
			try {
				String remotePort = String.valueOf((Integer.parseInt(msgs[1]) * 2));
				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(remotePort));
				socket.setTcpNoDelay(true);
				socket.setSoTimeout(3000);

				String msgToSend = msgs[0];
				DataOutputStream out = new DataOutputStream(socket.getOutputStream());
				out.writeUTF(msgToSend);
				out.flush();

				DataInputStream in = new DataInputStream(socket.getInputStream());
				String reply = in.readUTF();
				in.close();

				Log.d(TAG, myPort + " : Client: Msg: " + msgs[0] + " Receiver: " + msgs[1]);
				status = reply;
			} catch (SocketTimeoutException timeoutException) {
				Log.e(TAG, "Socket timed out. Socket " + msgs[1] + " failed.");
				status = statusFailure;
			}
			catch (IOException e) {
				Log.e(TAG, myPort + " : Client: Msg: " + msgs[0] + " Receiver: " + msgs[1]);
				Log.e(TAG, "ClientTask socket IOException " + count);
				count++;
//				e.printStackTrace();
				status = statusFailure;
			}
			return status;
		}
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {

            recoveryHandler();
			ServerSocket serverSocket = sockets[0];
			Socket socket = null;

			while (true) {
				try {
					socket = serverSocket.accept();
					DataOutputStream out = new DataOutputStream(socket.getOutputStream());
					DataInputStream in = new DataInputStream(socket.getInputStream());
					String msg = in.readUTF().trim();
					String message[] = msg.split(":");
					String msgType = message[0];

					if(isRecoveryInProgress) {
						if(msgType.equals(msgTypeInsertReplicaReq)) {
							Log.e(TAG, myPort + ": Inserting Key to Queue: " + msg);
							holdBackQueue.add(msg);
							out.writeUTF(statusFailure);
							out.flush();
							in.close();
							socket.close();
						} else if(msgType.equals(msgTypeQueryReq)) {
							out.writeUTF(statusFailure);
							out.flush();
							in.close();
							socket.close();
						} else {
							out.writeUTF(statusSuccess);
							out.flush();
							in.close();
							socket.close();
							publishProgress(msg);
						}
					} else {
						if(msgType.equals(msgTypeQueryReq)) {
							String selection = message[1];
							String coordinator = message[2];
							String queryResMsg = fetchQuery(null, selection, null, null, coordinator, false);
							out.writeUTF(queryResMsg);
						} else {
							out.writeUTF(statusSuccess);
							out.flush();
							publishProgress(msg);
						}
						in.close();
						socket.close();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		protected void onProgressUpdate(String...strings) {
			String msg = strings[0].trim();
			String message[] = msg.split(":");
			String msgType = message[0];

			if(msgType.equals(msgTypeInsertReplicaReq)) {
				String key = message[1];
				String value = message[2];
				String coordinator = message[3];
				ContentValues toInsert = new ContentValues();
				toInsert.put(DatabaseContract.Messenger.COLUMN_NAME_MESSAGE_KEY, key);
				toInsert.put(DatabaseContract.Messenger.COLUMN_NAME_MESSAGE_VALUE, value);
				long id = performInsert(toInsert);
				String status = statusFailure;
				if(id != -1) {
					status = statusSuccess;
				}
				String insertReplicaResMsg = msgTypeInsertReplicaRes + ":" + status;
				notifyCoordinator(insertReplicaResMsg, coordinator);
			} else if(msgType.equals(msgTypeInsertReplicaRes)) {
				Log.d(TAG, "Replica inserted successfully");
				String status = message[1];
//				synchronized (insertReplicaLock) {
//					if(status.equals(statusSuccess)) {
//						insertReplicaLock.notify();
//					}
//				}
			} else if(msgType.equals(msgTypeQueryReq)) {
				String selection = message[1];
				String coordinator = message[2];
				DatabaseHelper databaseHelper = DatabaseHelper.getInstance(getContext());
				messageDB = databaseHelper.getReadableDatabase();
				fetchQuery(null, selection, null, null, coordinator, true);
			} else if(msgType.equals(msgTypeQueryRes)) {
				String sender = message[1];
				String key = message[2];
				String value = message[3];
				queryResult.addRow(new Object[] {key, value});
				synchronized (queryLock) {
					queryLock.notify();
				}
			} else if(msgType.equals(msgTypeQueryAllReq)) {
				String initiator = message[1];
				DatabaseHelper databaseHelper = DatabaseHelper.getInstance(getContext());
				messageDB = databaseHelper.getWritableDatabase();
				fetchQueryAll(null, null, null, initiator);
			} else if(msgType.equals(msgTypeQueryAllRes)) {
				String sender = message[1];
				if(message.length % 2 == 0) {
					for(int i = 2; i < message.length; i+=2) {
						String key = message[i];
						String value = message[i + 1];
						queryAllResult.addRow(new Object[] {key, value});
					}
				}
			} else if(msgType.equals(msgTypeDeleteReplicaReq)) {
				String selection = message[1];
				String coordinator = message[2];
				if(!myPort.equals(coordinator)) {
					DatabaseHelper databaseHelper = DatabaseHelper.getInstance(getContext());
					messageDB = databaseHelper.getReadableDatabase();
					performDelete(selection, coordinator);
				} else {
					synchronized (deleteLock) {
						deleteLock.notify();
					}
				}
			} else if(msgType.equals(msgTypeDeleteRes)) {
				String sender = message[1];
				String count = message[2];
				deleteCount += Integer.parseInt(count);
				synchronized (deleteLock) {
					deleteLock.notify();
				}
			} else if(msgType.equals(msgTypeDeleteAllReq)) {
				String coordinator = message[1];
				DatabaseHelper databaseHelper = DatabaseHelper.getInstance(getContext());
				messageDB = databaseHelper.getWritableDatabase();
				performDeleteAll(coordinator);
			} else if(msgType.equals(msgTypeDeleteAllRes)) {
				String sender = message[1];
				String count = message[2];
				deleteAllCount += Integer.parseInt(count);
			} else if(msgType.equals(msgTypeRecoveryReq)) {
				String initiator = message[1];
				recoveryFetchAll(initiator);
			} else if(msgType.equals(msgTypeRecoveryRes)) {
				try {
					DatabaseHelper databaseHelper = DatabaseHelper.getInstance(getContext());
					messageDB = databaseHelper.getWritableDatabase();
					List<String> validNodes = predecessorMap.get(myPort);
					Log.e(TAG, myPort + ": Recovery! Count:" + recoveryResponseCount);
					for(int i = 2; i < message.length; i+=2) {
						String key = message[i];
						String value = message[i + 1];
						ContentValues toInsert = new ContentValues();
						toInsert.put(DatabaseContract.Messenger.COLUMN_NAME_MESSAGE_KEY, key);
						toInsert.put(DatabaseContract.Messenger.COLUMN_NAME_MESSAGE_VALUE, value);
						if(validNodes.contains(getRecipient(genHash(key)))) {
							performInsert(toInsert);
						}
					}
					recoveryResponseCount += 1;
					if(recoveryResponseCount == 4) {
						Log.e(TAG, myPort + ": Received all responses. Hence now getting from queue");
						while(!holdBackQueue.isEmpty()) {
							String storedMsg = holdBackQueue.poll();
							String[] storedMessage = storedMsg.split(":");
							String key = storedMessage[1];
							String value = storedMessage[2];
							String coordinator = storedMessage[3];
							Log.e(TAG, myPort + ": Inserting Key From Queue: " + key + " Value: " + value);
							ContentValues toInsert = new ContentValues();
							toInsert.put(DatabaseContract.Messenger.COLUMN_NAME_MESSAGE_KEY, key);
							toInsert.put(DatabaseContract.Messenger.COLUMN_NAME_MESSAGE_VALUE, value);
							if(validNodes.contains(getRecipient(genHash(key)))) {
								performInsert(toInsert);
							}
						}
						isRecoveryInProgress = false;
						recoveryResponseCount = 0;
					}
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private String getRecipient(String hashedKey) {
		String key = hashedNodeList.ceilingKey(hashedKey);
		if(key == null) {
			key = hashedNodeList.firstKey();
		}
		return hashedNodeList.get(key);
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		String status = processInsert(values, myPort);
		if(status.equals(statusSuccess)) {
			Log.e(TAG, myPort + ": Insert successful");
		} else {
			Log.e(TAG, myPort + ": Insert failed");
		}
		return uri;
	}

	private String processInsert(ContentValues values, String coordinator) {
		String inKey = String.valueOf(values.get(DatabaseContract.Messenger.COLUMN_NAME_MESSAGE_KEY));
		String inValue = String.valueOf(values.get(DatabaseContract.Messenger.COLUMN_NAME_MESSAGE_VALUE));
		String status = statusFailure;
		try {
			String hashedInKey = genHash(inKey);
			String recipient = getRecipient(hashedInKey);
			List<String> replicaPorts = replicaMap.get(recipient);
			if(replicaPorts != null && replicaPorts.size() > 0) {
				String insertReplicaMsgReq = msgTypeInsertReplicaReq + ":" + inKey + ":" + inValue + ":" + coordinator + ":" + recipient;
				for(String port : replicaPorts) {
					if(port.equals(myPort) && !isRecoveryInProgress) {
						performInsert(values);
					} else {
//						synchronized (insertReplicaLock) {
							Log.e(TAG, myPort + ": Replicate to replica group member: " + port);
							status = processReplication(insertReplicaMsgReq, port);
//							if(status.equals(statusSuccess)) {
//								insertReplicaLock.wait(3000);
//							}
//						}
					}
				}

			}
		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "No such algorithm exception");
			e.printStackTrace();
		} catch (InterruptedException e) {
			Log.e(TAG, "Interrupted Exception");
			e.printStackTrace();
		} catch (ExecutionException e) {
			Log.e(TAG, "Concurrent execution exception");
			e.printStackTrace();
		}
		return status;
	}

	private long performInsert(ContentValues values) {
		return messageDB.insertWithOnConflict(DatabaseContract.Messenger.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
	}

	private String processReplication(String msg, String recipient) throws InterruptedException, ExecutionException {
		return (new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, recipient)).get();
	}

	private void notifyCoordinator(String message, String coordinator) {
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, coordinator);
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		DatabaseHelper databaseHelper = DatabaseHelper.getInstance(getContext());
		messageDB = databaseHelper.getReadableDatabase();

		if(selection.equals("@")) {
            while(isRecoveryInProgress){
                //Waiting for the recovery to complete
                 }
			return messageDB.query(DatabaseContract.Messenger.TABLE_NAME, DatabaseContract.Messenger.PROJECTION, null, selectionArgs, sortOrder, null, null);
		} else if(selection.equals("*")) {
			processQueryAll(projection, selectionArgs, sortOrder, myPort);
			return queryAllResult;
		} else {
			return processQuery(projection, selection, selectionArgs, sortOrder, myPort);
		}
	}

	private void processQueryAll(String[] projection, String[] selectionArgs, String sortOrder, String initiator) {
		try {
			Cursor output = messageDB.query(DatabaseContract.Messenger.TABLE_NAME, DatabaseContract.Messenger.PROJECTION, null, selectionArgs, sortOrder, null, null);
			queryAllResult = new MatrixCursor(new String[] {DatabaseContract.Messenger.COLUMN_NAME_MESSAGE_KEY, DatabaseContract.Messenger.COLUMN_NAME_MESSAGE_VALUE});

			while(output.moveToNext()) {
				String key = output.getString(output.getColumnIndex(DatabaseContract.Messenger.COLUMN_NAME_MESSAGE_KEY));
				String value = output.getString(output.getColumnIndex(DatabaseContract.Messenger.COLUMN_NAME_MESSAGE_VALUE));
				queryAllResult.addRow(new Object[] {key, value});
			}
			output.close();

			for(String node: dynamoRing) {
				if(!node.equals(myPort)) {
					synchronized (queryAllLock) {
						String queryAllReqMsg = msgTypeQueryAllReq + ":" + initiator;
						String status = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryAllReqMsg, node).get();
						if(status.equals(statusSuccess)) {
							queryAllLock.wait(3000);
						}
					}
				}
			}
		} catch (InterruptedException e) {
			Log.e(TAG, "Interrupted Exception");
			e.printStackTrace();
		} catch (ExecutionException e) {
			Log.e(TAG, "Concurrent execution exception");
			e.printStackTrace();
		}
	}

	private void fetchQueryAll(String[] projection, String[] selectionArgs, String sortOrder, String initiator) {
		Cursor output = messageDB.query(DatabaseContract.Messenger.TABLE_NAME, DatabaseContract.Messenger.PROJECTION, null, selectionArgs, sortOrder, null, null);
		String queryMsgRes = msgTypeQueryAllRes + ":" + myPort;
		StringBuilder msg = new StringBuilder(queryMsgRes);
		while(output.moveToNext()) {
			String key = output.getString(output.getColumnIndex(DatabaseContract.Messenger.COLUMN_NAME_MESSAGE_KEY));
			String value = output.getString(output.getColumnIndex(DatabaseContract.Messenger.COLUMN_NAME_MESSAGE_VALUE));
			String keyValue = ":" + key + ":" + value;
			msg.append(keyValue);
		}
		output.close();
		queryMsgRes = msg.toString();
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryMsgRes, initiator);
	}

	private void recoveryFetchAll(String initiator) {
		Cursor output = messageDB.query(DatabaseContract.Messenger.TABLE_NAME, null, null, null, null, null, null);
		String recoveryMsgRes = msgTypeRecoveryRes + ":" + myPort;
		StringBuilder msg = new StringBuilder(recoveryMsgRes);
		while(output.moveToNext()) {
			String key = output.getString(output.getColumnIndex(DatabaseContract.Messenger.COLUMN_NAME_MESSAGE_KEY));
			String value = output.getString(output.getColumnIndex(DatabaseContract.Messenger.COLUMN_NAME_MESSAGE_VALUE));
			String keyValue = ":" + key + ":" + value;
			msg.append(keyValue);
		}
		output.close();
		recoveryMsgRes = msg.toString();
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, recoveryMsgRes, initiator);
	}

	private Cursor processQuery(String[] projection, String selection, String[] selectionArgs, String sortOrder, String initiator) {
		synchronized (queryLock) {
			try {
				String hashedInKey = genHash(selection);
				String recipient = getRecipient(hashedInKey);
				List<String> replicaPorts = replicaMap.get(recipient);
				String queryReqMsg = msgTypeQueryReq + ":" + selection + ":" + initiator;
				for (String port : replicaPorts) {
					if (port.equals(myPort) && !isRecoveryInProgress) {
						String _selection = DatabaseContract.Messenger.COLUMN_NAME_MESSAGE_KEY + "='" + selection + "'";
						return messageDB.query(DatabaseContract.Messenger.TABLE_NAME, DatabaseContract.Messenger.PROJECTION, _selection, selectionArgs, sortOrder, null, null);
					} else {
						Log.d(TAG, myPort + ": Querying " + port + " to get for key " + selection);
						String status = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryReqMsg, port).get();
						if (!status.equals(statusFailure)) {
							queryResult = new MatrixCursor(new String[]{DatabaseContract.Messenger.COLUMN_NAME_MESSAGE_KEY, DatabaseContract.Messenger.COLUMN_NAME_MESSAGE_VALUE});
							String[] message = status.split(":");
							String key = message[2];
							String value = message[3];
							queryResult.addRow(new Object[]{key, value});
							return queryResult;
						} else {
							Log.e(TAG, myPort + ": Querying " + port + " to get for key " + selection + status);
						}
					}
				}
			} catch(InterruptedException e){
				Log.e(TAG, "Interrupted Exception");
				e.printStackTrace();
			} catch(ExecutionException e){
				Log.e(TAG, "Concurrent execution exception");
				e.printStackTrace();
			} catch(NoSuchAlgorithmException e){
				Log.e(TAG, "No such algorithm exception");
				e.printStackTrace();
			}
		}
	return queryResult;
	}

	private String fetchQuery(String[] projection, String selection, String[] selectionArgs, String sortOrder, String coordinator, boolean notifyCoordinator) {
		String _selection = DatabaseContract.Messenger.COLUMN_NAME_MESSAGE_KEY + "='" + selection + "'";
		Cursor output =  messageDB.query(DatabaseContract.Messenger.TABLE_NAME, DatabaseContract.Messenger.PROJECTION, _selection, selectionArgs, sortOrder, null, null);

		String key = "";
		String value = "";
		while(output.moveToNext()) {
			key = output.getString(output.getColumnIndex(DatabaseContract.Messenger.COLUMN_NAME_MESSAGE_KEY));
			value = output.getString(output.getColumnIndex(DatabaseContract.Messenger.COLUMN_NAME_MESSAGE_VALUE));
		}
		output.close();
		String queryResMsg = msgTypeQueryRes + ":" + myPort + ":" + key + ":" + value;
		if(notifyCoordinator) {
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryResMsg, coordinator);
		}
		return queryResMsg;
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		DatabaseHelper databaseHelper = DatabaseHelper.getInstance(getContext());
		messageDB = databaseHelper.getWritableDatabase();
		if(selection.equals("@")) {
			Cursor deleteCursor = messageDB.query(DatabaseContract.Messenger.TABLE_NAME, DatabaseContract.Messenger.PROJECTION, null, selectionArgs, null, null, null);
			int localDeleteCount = 0;
			while(deleteCursor.moveToNext()) {
				String _selection = deleteCursor.getString(deleteCursor.getColumnIndex(DatabaseContract.Messenger.COLUMN_NAME_MESSAGE_KEY));
				processDelete(_selection, myPort);
				localDeleteCount++;
			}
			return localDeleteCount;
		} else if(selection.equals("*")) {
			processDeleteAll(selectionArgs, myPort);
			return deleteAllCount;
		} else {
			return processDelete(selection, myPort);
		}
	}

	private void processDeleteAll(String[] selectionArgs, String initiator) {
		int count = messageDB.delete(DatabaseContract.Messenger.TABLE_NAME,null,selectionArgs);
		deleteAllCount += count;
		try {
			for(String node: dynamoRing) {
				if(!node.equals(myPort)) {
					synchronized (deleteAllLock) {
						String deleteAllReqMsg = msgTypeDeleteAllReq + ":" + initiator;
						String status = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteAllReqMsg, node).get();
						if(status.equals(statusSuccess)) {
							deleteAllLock.wait(3000);
						}
					}
				}
			}
		} catch(InterruptedException e) {
			Log.e(TAG, "Interrupted Exception");
			e.printStackTrace();
		} catch (ExecutionException e) {
			Log.e(TAG, "Concurrent execution exception");
			e.printStackTrace();
		}
	}

	private void performDeleteAll(String coordinator) {
		int count = messageDB.delete(DatabaseContract.Messenger.TABLE_NAME, null, null);
		String deleteAllMsgRes = msgTypeDeleteAllRes + ":" + myPort + ":" + count;
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteAllMsgRes, coordinator);
	}

	private int processDelete(String selection, String coordinator) {
		try {
			String hashedInKey = genHash(selection);
			String recipient = getRecipient(hashedInKey);

			synchronized (deleteLock) {
				List<String> replicaPorts = replicaMap.get(recipient);
				if(replicaPorts != null && replicaPorts.size() > 0) {
					String deleteReplicaMsgReq = msgTypeDeleteReplicaReq + ":" + selection + ":" + coordinator;
					for(String port : replicaPorts) {
						String status = processReplication(deleteReplicaMsgReq, port);
						if(status.equals(statusSuccess)) {
							deleteLock.wait(3000);
						}
					}
				}
			}
		} catch(InterruptedException e) {
			Log.e(TAG, "Interrupted Exception");
			e.printStackTrace();
		} catch (ExecutionException e) {
			Log.e(TAG, "Concurrent execution exception");
			e.printStackTrace();
		} catch(NoSuchAlgorithmException e) {
			Log.e(TAG, "No such algorithm exception");
			e.printStackTrace();
		}
		return deleteCount;
	}

	private void performDelete(String selection, String coordinator) {
		String _selection = DatabaseContract.Messenger.COLUMN_NAME_MESSAGE_KEY + "='" + selection + "'";
		int count = messageDB.delete(DatabaseContract.Messenger.TABLE_NAME, _selection, null);

		String deleteMsgRes = msgTypeDeleteRes + ":" + myPort + ":" + count;
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteMsgRes, coordinator);
	}

	public void recoveryHandler() {
		try {
			Log.e(TAG, "Recovery has started!");
			ContentValues contentValues = new ContentValues();
			contentValues.put(DatabaseContract.Messenger.COLUMN_NAME_MESSAGE_KEY, "Validation_Key");
			contentValues.put(DatabaseContract.Messenger.COLUMN_NAME_MESSAGE_VALUE, "Validation_Value");
			messageDB.insert(DatabaseContract.Messenger.TABLE_NAME, null, contentValues);
			Cursor output = messageDB.query(DatabaseContract.Messenger.TABLE_NAME, DatabaseContract.Messenger.PROJECTION, null, null, null, null, null);
			if(output.getCount() > 1) {
				isRecoveryInProgress = true;
				messageDB.delete(DatabaseContract.Messenger.TABLE_NAME, null, null);
				List<String> recipients = replicaMap.get(myPort);
				Log.e(TAG, myPort + " is recovering!");
				for(String node : recipients) {
					if(!node.equals(myPort)) {
						Log.e(TAG, myPort + ": Recovery req to " + node + " to get my data");
						String recoveryReqMsg = msgTypeRecoveryReq + ":" + myPort;
						String status = (new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, recoveryReqMsg, node)).get();
					}
				}
				List<String> predRecipients = predecessorMap.get(myPort);
				for(String node : predRecipients) {
					if(!node.equals(myPort)) {
						Log.e(TAG, myPort + ": Recovery req to " + node + " to get replica data");
						String recoveryReqMsg = msgTypeRecoveryReq + ":" + myPort;
						String status = (new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, recoveryReqMsg, node)).get();
						if(status.equals(statusFailure)) {
							List<String> recoveryNodeList = replicaMap.get(node);
							for(String everyNode : recoveryNodeList) {
								if(!everyNode.equals(myPort)) {
									new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, recoveryReqMsg, everyNode);
									break;
								}
							}
						}
					}
				}
			} else {
				messageDB.delete(DatabaseContract.Messenger.TABLE_NAME, null, null);
			}
			output.close();
		} catch(InterruptedException e) {
			Log.e(TAG, "Interrupted Exception");
			e.printStackTrace();
		} catch (ExecutionException e) {
			Log.e(TAG, "Concurrent execution exception");
			e.printStackTrace();
		}
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
