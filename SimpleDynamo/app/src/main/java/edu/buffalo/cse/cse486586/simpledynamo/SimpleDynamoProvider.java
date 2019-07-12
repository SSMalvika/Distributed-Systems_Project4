package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


/************************************************************************************
 * ---------------------------------
 * Overall References Used
 * ----------------------------------
 *
 * @author Malvika
 * 
 * 1)Code snippets from PA-2B and PA-3 were used
 *
 * 2)https://stackoverflow.com/questions/683041/how-do-i-use-a-priorityqueue
 * The above reference was used to understand how to implement the comparator
 * of a priority queue and the same logic is implemented from the link
 *
 * 3)https://docs.oracle.com/javase/7/docs/api/java/util/Comparator.html
 *Reference documentation for understanding the implementation of a comparator
 *
 * 4)https://docs.oracle.com/javase/8/docs/api/java/util/HashMap.html
 *    The above link was used to understand the implementation of HashMap and its member functions
 *    Hashmap is used to store port as key and emultaor id as value
 *
 * 5) https://docs.oracle.com/javase/7/docs/api/java/util/ArrayList.html
 *    The above link was used to understand the implementation of Arraylist
 *
 * 6)https://docs.oracle.com/javase/7/docs/api/java/util/Collections.html
 *    The above link was used to understand sorting of arraylist for Java versions before 8 and how to sort with comparator
 *
 * 7)https://stackoverflow.com/questions/8725387/why-is-there-no-sortedlist-in-java
 *    The above link was used to learn about sorted data structures in Java and learnt that List can be sorted from the
 *    Collections sort method
 *
 * 8)https://docs.oracle.com/javase/8/docs/api/java/sql/Timestamp.html
 *    The above link was used to learn about the use of timestamps for implementing versioning
 *
 * 9)https://docs.oracle.com/javase/8/docs/api/java/util/HashMap.html
 *    The above link was used to understand the implementation of HashMap and its member functions
 *    Hashmap is used to store port as key and emultaor id as value
 *
 * 10) https://docs.oracle.com/javase/7/docs/api/java/util/ArrayList.html
 *    The above link was used to understand the implementation of Arraylist
 *
 * 11)https://docs.oracle.com/javase/7/docs/api/java/util/Collections.html
 *    The above link was used to understand sorting of arraylist for Java versions before 8 and how to sort with comparator
 *
 * 12)https://stackoverflow.com/questions/8725387/why-is-there-no-sortedlist-in-java
 *    The above link was used to learn about sorted data structures in Java and learnt that List can be sorted from the
 *    Collections sort method
 *
 *     REFERENCES FOR DELETE:
 * 13) https://developer.android.com/reference/java/io/File.html#delete()
 *    The above link was used to understand the delete function of files
 * 14)https://developer.android.com/reference/java/io/File.html#listFiles()
 *    The above link was used as reference to understand the function that lists the directory and respective files in that directory
 *
 * 15)https://developer.android.com/reference/android/content/Context
 *    The above link was used to learn about the method deleteFile that's in context
 *
 *    REFERENCES:
 *               ------------
 * 16)PA - 1 to learn how to use files and use them
 * 17)https://developer.android.com/reference/android/content/Context
 *    To understand the concept of context and for using openFileOutput, and also to check various write
 *    modes which provides delete contents and overwrite(mode is MODE_PRIVATE)
 * 18)https://developer.android.com/reference/java/io/FileInputStream.html
 *    To understand FileInputStream and its member functions,to read the file contents which is in bytes and convert them to string.
 *
 * 19)https://developer.android.com/reference/java/io/FileInputStream.html#available()
 *    The available method returns the number of bytes available to read and the byte array is converted to string
 *
 * 20)http://developer.android.com/reference/android/database/MatrixCursor.html
 *    To understand how to use matrix cursor and the function addRow
 *
 *       OBJECT INPUT AND OUTPUT STREAM REFERENCES
 *  21)https://stackoverflow.com/questions/12895450/sending-an-arrayliststring-from-the-server-side-to-the-client-side-over-tcp-us
 *      The above link was used to understand the use of object input and output stream, the order in which it should be used and
 *      the way to pass arraylist as object to the streams and retrieve back
 *
 *  22)https://docs.oracle.com/javase/7/docs/api/java/io/ObjectOutputStream.html
 *      The above documentation was used to know about the member functions of objectoutputstream
 *
 *  23)https://docs.oracle.com/javase/7/docs/api/java/io/ObjectInputStream.html
 *      The above documentation was used to know about the member functions of objectinputstream
 *
 *      MATRIX CURSOR REFERENCE
 *  24)https://developer.android.com/reference/android/database/MatrixCursor
 *  25)https://developer.android.com/reference/android/database/Cursor
 *      References for the use of  Matrix cursor and cursor implementations
 *
 *  26)https://stackoverflow.com/questions/4396604/how-to-solve-cursorindexoutofboundsexception
 *      The above reference was to understand the exception thrown with matrixcursor and how to iterate through the
 *      values of the cursor using the movement of cursor pointer
 *
 *  27)https://developer.android.com/reference/android/content/ContentValues
 *      The above link was used for the understanding of contentvalues
 *
 *

**************************************************************************************/



//RingNode for handling node joins
class RingNode
{
	String hash;
	int emul_id;
	int port;
	RingNode(String genhash,int id,int prt)
	{
		hash=genhash;
		emul_id=id;
		port=prt;
	}
}
//Ref - https://stackoverflow.com/questions/683041/how-do-i-use-a-priorityqueue
//Comparator to sort the chordnode values in arraylist based on the hash value of emulator-id
class RingComp implements Comparator<RingNode> {
	public int compare(RingNode s1, RingNode s2) {
		if (s1.hash.compareToIgnoreCase(s2.hash) < 0 )
		{
			return -1;
		}
		else if (s1.hash.compareToIgnoreCase(s2.hash) > 0)
		{
			return 1;
		}
		else
		{
			return 0;
		}

	}
}

public class SimpleDynamoProvider extends ContentProvider {
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();

	HashMap<String,String> EMUL_PORTS=new HashMap<String, String>(5);

	final ReentrantLock lock = new ReentrantLock();

	List<RingNode> dynamolist=new ArrayList<RingNode>();
	RingComp cmp=new RingComp();

	static final int SERVER_PORT = 10000;

	//The below variables are used for information about successor,predecessor and about the head node
	int my_port=0,predecessor_port=0;
	Boolean isHead=false;
	List<Integer> ringports=new ArrayList<Integer>();

	//Declaring the key and value field
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";


	private void insertChordList(RingNode newdynamonode)
	{
		//Ref: https://stackoverflow.com/questions/8725387/why-is-there-no-sortedlist-in-java
		dynamolist.add(newdynamonode);
		Collections.sort(dynamolist,cmp);
	}
	//Uri Builder module to build the uri in the required format for Content Provider
	//The module is taken from the OnPTestClickListener Code
	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
	private void delfiles()
	{
		File op[] = getContext().getFilesDir().listFiles();
		for (int i = 0; i < op.length; i++) {
			if (op[i].exists()) {
				op[i].delete();
				Log.i("File Deleted", op[i].getAbsolutePath());
			}
		}
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		if (selection.contentEquals(("@"))) {
			delfiles();
		} else if (selection.contentEquals("*")) {
			//to implement delete on all content providers from the list
			for(int j=0;j<ringports.size();j++) {
				if (my_port != ringports.get(j)) {
					String delmes = "Delete".concat(selection);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(ringports.get(j)), delmes);
				}
			}
			delfiles();

		}else {
				try {
					String idhash = genHash(selection);
					String predhash;
					String delmess = "Delete:Key-".concat(selection);
					for(int i=0;i<dynamolist.size();i++) {
						String myhash = dynamolist.get(i).hash;
						if(i==0)
						{
							predhash=dynamolist.get(dynamolist.size()-1).hash;
						}else
						{
							predhash=dynamolist.get(i-1).hash;
						}
						if ((idhash.compareToIgnoreCase(predhash) > 0 && idhash.compareToIgnoreCase(myhash) <= 0)|| (idhash.compareToIgnoreCase(predhash)>0 && (i==0)) || (idhash.compareToIgnoreCase(predhash)<0 && idhash.compareToIgnoreCase(myhash)<=0 && (i==0)))
						{

							String value="";
							long tm1;
							Timestamp tmp1=new Timestamp(System.currentTimeMillis());
							if(my_port==dynamolist.get(i).port) {
								try {

									getContext().deleteFile(selection);
									Log.i("File Deleted",selection);
								}catch(Exception e)
								{
									Log.e("Error", "File not found");
								}

							}else {

								// Ref: https://stackoverflow.com/questions/14045765/waiting-for-asynctask-to-finish-or-variable-to-be-set
								new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(dynamolist.get(i).port), delmess);

							}

							int succ_port1=0,succ_port2=0;
							if(i==dynamolist.size()-2)
							{
								succ_port1=dynamolist.get(i+1).port;
								succ_port2=dynamolist.get(0).port;
							}
							else if(i==dynamolist.size()-1)
							{
								succ_port1=dynamolist.get(0).port;
								succ_port2=dynamolist.get(1).port;
							}
							else
							{
								succ_port1=dynamolist.get(i+1).port;
								succ_port2=dynamolist.get(i+2).port;
							}

							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,Integer.toString(succ_port1),delmess);
							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,Integer.toString(succ_port2),delmess);
							break;
						}
					}




		}catch(Exception e)
		{
			Log.e(TAG,"Hash Function Error");
		}

		}
		return 0;
	}



	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	private void insertfiles(String value,String filename)
	{
		FileOutputStream outputStream;
		try {
			outputStream = getContext().openFileOutput(filename,Context.MODE_PRIVATE);
			//Ref: https://docs.oracle.com/javase/8/docs/api/java/sql/Timestamp.html
			Timestamp tmp=new Timestamp(System.currentTimeMillis());
			long tmval=tmp.getTime();
			value=value.concat(":TIME:").concat(Long.toString(tmval));
			outputStream.write(value.getBytes());
			outputStream.close();
			Log.i("Insert",filename);
		} catch (Exception e) {
			Log.e("Error", "File write failed");
		}

	}


	private synchronized void insertrecoveryfiles(String value,String filename)
	{
		//Recovery Process
		lock.lock();  // block until condition holds
		try {
			FileOutputStream outputStream;
			try {
				File op=getContext().getFileStreamPath(filename);
				if(op.exists())
				{
					FileInputStream in = getContext().openFileInput(filename);
					int n = in.available();
					byte[] result = new byte[n];
					in.read(result);
					String val2 = new String(result);
					//Ref: https://docs.oracle.com/javase/8/docs/api/java/sql/Timestamp.html
					Timestamp tmp=new Timestamp(Long.parseLong(val2.split(":TIME:")[1]));
					Timestamp tmp2=new Timestamp(Long.parseLong(value.split(":TIME:")[1]));
					if(tmp2.after(tmp))
					{
						outputStream = getContext().openFileOutput(filename,Context.MODE_PRIVATE);
						outputStream.write(value.getBytes());
						outputStream.close();
					}

				}else {

					outputStream = getContext().openFileOutput(filename,Context.MODE_PRIVATE);
					outputStream.write(value.getBytes());
					outputStream.close();
				}

			} catch (Exception e) {
				Log.e("Error", "File write failed");
			}
		} finally {
			lock.unlock();

		}


	}
	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		//Values are retrieved from the keys(which is key and value) and written to the file
		String value = values.getAsString(VALUE_FIELD);
		String filename = values.getAsString(KEY_FIELD);
		Log.i("INSERT-CHECK",filename);
		try {
			String idhash = genHash(filename);
			String predhash;
			for(int i=0;i<dynamolist.size();i++) {
				String myhash = dynamolist.get(i).hash;
				if(i==0)
				{
					predhash=dynamolist.get(dynamolist.size()-1).hash;
				}else
				{
					predhash=dynamolist.get(i-1).hash;
				}
				if ((idhash.compareToIgnoreCase(predhash) > 0 && idhash.compareToIgnoreCase(myhash) <= 0) || (idhash.compareToIgnoreCase(predhash)>0 && (i==0)) || (idhash.compareToIgnoreCase(predhash)<0 && idhash.compareToIgnoreCase(myhash)<=0 && (i==0)))
				{
					String insertdata = "INSERT:KEY-".concat(filename).concat(";VALUE-").concat(value);
					if(my_port==dynamolist.get(i).port) {
						Timestamp tmp=new Timestamp(System.currentTimeMillis());
						long tmval=tmp.getTime();
						value=value.concat(":TIME:").concat(Long.toString(tmval));
						insertrecoveryfiles(value, filename);
					}else {

						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(dynamolist.get(i).port),insertdata);
					}
					int succ_port1=0,succ_port2=0;
					if(i==dynamolist.size()-2)
					{
						succ_port1=dynamolist.get(i+1).port;
						succ_port2=dynamolist.get(0).port;
					}
					else if(i==dynamolist.size()-1)
					{
						succ_port1=dynamolist.get(0).port;
						succ_port2=dynamolist.get(1).port;
					}
					else
					{
						succ_port1=dynamolist.get(i+1).port;
						succ_port2=dynamolist.get(i+2).port;
					}

					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,Integer.toString(succ_port1),insertdata);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,Integer.toString(succ_port2),insertdata);

					break;
				}
			}
			return uri;
		}catch(Exception e)
		{
			Log.e(TAG,"Hash Function Error 1");
		}

		return null;
	}

	private synchronized void recoverFiles()
	{
		final Uri mUri= buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
		delete(mUri,"@",null);
		int pred_port1=dynamolist.get(0).port;
		int pred_port2=0,succ_port1=0,succ_port2=0;
		for(int i=0;i<dynamolist.size();i++)
		{
			if(dynamolist.get(i).port==my_port)
			{
				if(i==0)
				{
					pred_port1=dynamolist.get(dynamolist.size()-1).port;
					pred_port2=dynamolist.get(dynamolist.size()-2).port;
				}
				else if(i==1)
				{
					pred_port1=dynamolist.get(i-1).port;
					pred_port2=dynamolist.get(dynamolist.size()-1).port;
				}
				else
				{
					pred_port1=dynamolist.get(i-1).port;
					pred_port2=dynamolist.get(i-2).port;
				}
				if(i==dynamolist.size()-2)
				{
					succ_port1=dynamolist.get(i+1).port;
					succ_port2=dynamolist.get(0).port;
				}else if(i==dynamolist.size()-1)
				{
					succ_port1=dynamolist.get(0).port;
					succ_port2=dynamolist.get(1).port;
				}else
				{
					succ_port1=dynamolist.get(i+1).port;
					succ_port2=dynamolist.get(i+2).port;
				}
			}
		}

		//Inserting Predecessor Files
		String recpredmess = "RECOVER:PRED";
		List<String> reslist1 = null,reslist2=null;
		HashMap<String,String> insertsucc=new HashMap<String, String>();
		try {
			// Ref: https://stackoverflow.com/questions/14045765/waiting-for-asynctask-to-finish-or-variable-to-be-set
			reslist1 = new QueryStarTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(pred_port1),recpredmess).get();
			reslist2 = new QueryStarTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(pred_port2),recpredmess).get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		if (reslist1 != null) {
			for (int i = 0; i < reslist1.size(); i++) {
				String msgRcvd = reslist1.get(i);
				if (msgRcvd.contains("RecoverQuery:")) {
					String reskey = msgRcvd.split("Key-")[1].split(";")[0];
					String resval = msgRcvd.split("Value-")[1];
					insertsucc.put(reskey,resval);
				}
			}
		}
		if (reslist2 != null) {
			for (int i = 0; i < reslist2.size(); i++) {
				String msgRcvd = reslist2.get(i);
				if (msgRcvd.contains("RecoverQuery:")) {
					String reskey = msgRcvd.split("Key-")[1].split(";")[0];
					String resval = msgRcvd.split("Value-")[1];
					//resval=resval.split(":TIME:")[0];
					//insertrecoveryfiles(resval,reskey);
					insertsucc.put(reskey,resval);
				}
			}
		}

		//Inserting Successor Files
		String recsuccmess = "RECOVER:SUCC".concat(";Origin;").concat(Integer.toString(my_port));
		List<String> ressucclist1 = null,ressucclist2=null;
		try {
			// Ref: https://stackoverflow.com/questions/14045765/waiting-for-asynctask-to-finish-or-variable-to-be-set
			ressucclist1 = new QueryStarTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(succ_port1),recsuccmess).get();
			ressucclist2 = new QueryStarTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(succ_port2),recsuccmess).get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}

		if (ressucclist1 != null) {
			for (int i = 0; i < ressucclist1.size(); i++) {
				String msgRcvd = ressucclist1.get(i);
				if (msgRcvd.contains("RecoverQuery:")) {

					String reskey = msgRcvd.split("Key-")[1].split(";")[0];
					String resval = msgRcvd.split("Value-")[1];
					insertsucc.put(reskey,resval);
				}
			}
		}
		if (ressucclist2 != null) {
			for (int i = 0; i < ressucclist2.size(); i++) {
				String msgRcvd = ressucclist2.get(i);
				if (msgRcvd.contains("RecoverQuery:")) {

					String reskey = msgRcvd.split("Key-")[1].split(";")[0];
					String resval = msgRcvd.split("Value-")[1];
					long tm1=Long.parseLong(resval.split(":TIME:")[1]);
					Timestamp tmp1=new Timestamp(tm1);
					if(insertsucc.containsKey(reskey))
					{
						String val=insertsucc.get(reskey);
						long tm=Long.parseLong(val.split(":TIME:")[1]);
						Timestamp tmp=new Timestamp(tm);
						if(tmp1.after(tmp))
						{
							insertsucc.remove(reskey);
							insertsucc.put(reskey,resval);
						}
					}
				}
			}

			Iterator<String> keys=insertsucc.keySet().iterator();
			while(keys.hasNext())
			{
			   String key=keys.next();
			   String value=insertsucc.get(key);
			   insertrecoveryfiles(value,key);
			}
		}

        Log.i(TAG,"Recovery Complete");


	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

		//Initialising Hash Map
		EMUL_PORTS.put("11108","5554");
		EMUL_PORTS.put("11112","5556");
		EMUL_PORTS.put("11116","5558");
		EMUL_PORTS.put("11120","5560");
		EMUL_PORTS.put("11124","5562");

		//To get the information about the current AVD port from the telephony manager
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		my_port=Integer.parseInt(myPort);

		//Create a server task which listens on port 10000
		try {
			/*
			 * Create a server socket as well as a thread (AsyncTask) that listens on the server
			 * port.
			 *
			 * AsyncTask is a simplified thread construct that Android provides. Please make sure
			 * you know how it works by reading
			 * http://developer.android.com/reference/android/os/AsyncTask.html
			 */
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);


			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			/*
			 * Log is a good way to debug your code. LogCat prints out all the messages that
			 * Log class writes.
			 *
			 * Please read http://developer.android.com/tools/debugging/debugging-projects.html
			 * and http://developer.android.com/tools/debugging/debugging-log.html
			 * for more information on debugging.
			 */
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		}

		//Doing the static Join operation
		int start_emulid=5554;
		int end_emulid=5562;
		for(int i=start_emulid;i<=end_emulid;i+=2)
		{
			try {
				RingNode temp = new RingNode(genHash(Integer.toString(i)), i, i*2);
				insertChordList(temp);
			} catch (Exception e) {
				Log.e(TAG, "Hash Algorithm Exception");
			}
		}
		int len=dynamolist.size();
		if(len==5) {
			//New Join Nodes List
			for(int i=0;i<dynamolist.size();i++)
			{
				ringports.add(dynamolist.get(i).port);
				//Log.i(TAG,"JOIN "+Integer.toString(dynamolist.get(i).emul_id));
				if(my_port==ringports.get(i))
				{
					if(i==0)
					{
						predecessor_port=dynamolist.get(dynamolist.size()-1).port;
						isHead=true;

					}else
					{
						predecessor_port=dynamolist.get(i-1).port;
					}
				}
			}
		}

		//Recovery Process
		recoverFiles();




		return false;
	}
	private MatrixCursor queryfiles()
	{
		String[] columnNames={KEY_FIELD,VALUE_FIELD};
		MatrixCursor mc=new MatrixCursor(columnNames);

		File op[] = getContext().getFilesDir().listFiles();
		for (int i = 0; i < op.length; i++) {
			if (op[i].exists()) {
				try {
					FileInputStream in = getContext().openFileInput(op[i].getName());
					int n = in.available();
					byte[] result = new byte[n];
					in.read(result);
					String value = new String(result);
					value=value.split(":TIME:")[0];
					mc.addRow(new Object[]{op[i].getName(), value});
				}catch (Exception e)
				{
					Log.e(TAG,"File not Found");
				}
				Log.v("Query", op[i].getName());
			}
		}
		return mc;
	}

	private MatrixCursor queryrecoverfiles()
	{
		String[] columnNames={KEY_FIELD,VALUE_FIELD};
		MatrixCursor mc=new MatrixCursor(columnNames);
		File op[] = getContext().getFilesDir().listFiles();
		for (int i = 0; i < op.length; i++) {
			if (op[i].exists()) {
				try {
					FileInputStream in = getContext().openFileInput(op[i].getName());
					int n = in.available();
					byte[] result = new byte[n];
					in.read(result);
					String value = new String(result);
					mc.addRow(new Object[]{op[i].getName(), value});
				}catch (Exception e)
				{
					Log.e(TAG,"File not Found");
				}
				Log.v("Query", op[i].getName());
			}
		}
		return mc;
	}

	private MatrixCursor queryFile(String selection)
	{
		String[] columnNames={KEY_FIELD,VALUE_FIELD};
		MatrixCursor mc = new MatrixCursor(columnNames);
		try {
			FileInputStream in = getContext().openFileInput(selection);
			int n = in.available();
			byte[] result = new byte[n];
			in.read(result);
			String value = new String(result);
			mc.addRow(new Object[]{selection, value});
		} catch(Exception e){
			Log.e("Error", "File not found");
		}
		return mc;
	}
	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub

		String[] columnNames={KEY_FIELD,VALUE_FIELD};
		MatrixCursor mc=new MatrixCursor(columnNames);
		if(selection.contentEquals("@"))
		{
			mc=queryfiles();
			return mc;
		}
		else if(selection.contentEquals("*"))
		{
				MatrixCursor mcQuery = new MatrixCursor(columnNames);
				for(int j=0;j<ringports.size();j++) {

					if(my_port!=ringports.get(j)) {
						String querymess = "QUERY".concat(selection);
						List<String> reslist = null;
						try {
							// Ref: https://stackoverflow.com/questions/14045765/waiting-for-asynctask-to-finish-or-variable-to-be-set
							reslist = new QueryStarTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(ringports.get(j)), querymess).get();
						} catch (InterruptedException e) {
							e.printStackTrace();
						} catch (ExecutionException e) {
							e.printStackTrace();
						}
						if (reslist != null) {
							for (int i = 0; i < reslist.size(); i++) {
								String msgRcvd = reslist.get(i);
								if (msgRcvd.contains("StarQuery:")) {
									//  Log.i("RESULT", msgRcvd);
									String reskey = msgRcvd.split("Key-")[1].split(";")[0];
									String resval = msgRcvd.split("Value-")[1];
									resval=resval.split(":TIME:")[0];
									mcQuery.addRow(new Object[]{reskey, resval});
								}
							}
						}
					}
				}
				File op[] = getContext().getFilesDir().listFiles();
				for (int i = 0; i < op.length; i++) {
					if (op[i].exists()) {
						try {
							FileInputStream in = getContext().openFileInput(op[i].getName());
							int n = in.available();
							byte[] result = new byte[n];
							in.read(result);
							String value = new String(result);
							value=value.split(":TIME:")[0];
							mcQuery.addRow(new Object[]{op[i].getName(), value});
						}catch (Exception e)
						{
							Log.e(TAG,"File not Found");
						}
						Log.v("* Query", op[i].getName());
					}
				}
				return mcQuery;

		}
		else {

			try {
				String idhash = genHash(selection);
				String predhash;
				String querymess="QUERY:Key-".concat(selection);
				for(int i=0;i<dynamolist.size();i++) {
					String myhash = dynamolist.get(i).hash;
					if(i==0)
					{
						predhash=dynamolist.get(dynamolist.size()-1).hash;
					}else
					{
						predhash=dynamolist.get(i-1).hash;
					}
					if ((idhash.compareToIgnoreCase(predhash) > 0 && idhash.compareToIgnoreCase(myhash) <= 0)|| (idhash.compareToIgnoreCase(predhash)>0 && (i==0)) || (idhash.compareToIgnoreCase(predhash)<0 && idhash.compareToIgnoreCase(myhash)<=0 && (i==0)))
					{

						String value=null;
						long tm1;
						Timestamp tmp1=new Timestamp(System.currentTimeMillis());
						if(my_port==dynamolist.get(i).port) {
							try {
								FileInputStream in = getContext().openFileInput(selection);
								int n = in.available();
								byte[] result = new byte[n];
								in.read(result);
								value = new String(result);
								value=value.split(":TIME:")[0];
								tm1=Long.parseLong(value.split(":TIME:")[1]);
								tmp1=new Timestamp(tm1);
							} catch(Exception e){
								Log.e("Error", "File not found");
							}
							Log.v("Query", selection);
						}else {

							// Ref: https://stackoverflow.com/questions/14045765/waiting-for-asynctask-to-finish-or-variable-to-be-set
							String msgRcvd=new QueryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,Integer.toString(dynamolist.get(i).port),querymess).get();
							if(msgRcvd!=null) {
								if (msgRcvd.contains("ResultQuery:")) {
									Log.i("RESULT", msgRcvd);
									String reskey = msgRcvd.split("Key-")[1].split(";")[0];
									value = msgRcvd.split("Value-")[1];
									tm1 = Long.parseLong(value.split(":TIME:")[1]);
									value = value.split(":TIME:")[0];
									tmp1 = new Timestamp(tm1);
								}
							}
						}

						int succ_port1=0,succ_port2=0;
						if(i==dynamolist.size()-2)
						{
							succ_port1=dynamolist.get(i+1).port;
							succ_port2=dynamolist.get(0).port;
						}
						else if(i==dynamolist.size()-1)
						{
							succ_port1=dynamolist.get(0).port;
							succ_port2=dynamolist.get(1).port;
						}
						else
						{
							succ_port1=dynamolist.get(i+1).port;
							succ_port2=dynamolist.get(i+2).port;
						}

						String msgRep1=new QueryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,Integer.toString(succ_port1),querymess).get();
						String msgRep2=new QueryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,Integer.toString(succ_port2),querymess).get();

						if(msgRep1!=null)
						{
							if(msgRep1.contains("ResultQuery:"))
							{
								Log.i("RESULT1",msgRep1);
								String reskey=msgRep1.split("Key-")[1].split(";")[0];
								String rep1value=msgRep1.split("Value-")[1];
								long tmr1=Long.parseLong(rep1value.split(":TIME:")[1]);
								rep1value=rep1value.split(":TIME:")[0];
								Timestamp tmpr1=new Timestamp(tmr1);
								if(tmpr1.after(tmp1) || value==null)
								{
									value=rep1value;
									tmp1=new Timestamp(tmr1);
								}
							}
						}

						if(msgRep2!=null)
						{
							if(msgRep2.contains("ResultQuery:"))
							{
								Log.i("RESULT2",msgRep2);
								String reskey=msgRep2.split("Key-")[1].split(";")[0];
								String rep2value=msgRep2.split("Value-")[1];
								long tmr2=Long.parseLong(rep2value.split(":TIME:")[1]);
								rep2value=rep2value.split(":TIME:")[0];
								Timestamp tmpr2=new Timestamp(tmr2);
								if(tmpr2.after(tmp1) || value==null)
								{
									value=rep2value;
								}
							}
						}
						mc.addRow(new Object[]{selection, value});
						break;
					}
				}
				return mc;
			}catch(Exception e)
			{
				Log.e(TAG,"Hash Function Error 2");
			}
		}
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
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

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {

			ServerSocket serverSocket = sockets[0];
			final Uri mUri= buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
			try {
				//Server keeps listening for connections and accepting them and passing the message to Progress update
				while (true) {

					//Accepts an incoming client connection
					Socket client = serverSocket.accept();
					//Reads the message from the input stream and sends to Progress Update through publish progress
					BufferedReader input = new BufferedReader(new InputStreamReader(client.getInputStream()));
					String message = input.readLine();
					if(message.contains("INSERT"))
					{

						String key=message.split("KEY-")[1].split(";")[0];
						String value=message.split("VALUE-")[1].split(";")[0];
						Log.i(TAG,"INsert".concat(message));
						Timestamp tmp=new Timestamp(System.currentTimeMillis());
						long tmval=tmp.getTime();
						value=value.concat(":TIME:").concat(Long.toString(tmval));
						insertrecoveryfiles(value,key);
						PrintWriter outack= new PrintWriter(client.getOutputStream(), true);
						outack.println("ACK");
					}
					else if(message.contains("QUERY*"))
					{
						Cursor mcresult=query(mUri,null,"@",null,null);
						if(mcresult!=null)
						{
							Log.i("MCresult",Integer.toString(mcresult.getCount()));
							List<String> curresult=new ArrayList<String>();

							//Ref: https://stackoverflow.com/questions/4396604/how-to-solve-cursorindexoutofboundsexception
							if (mcresult.moveToNext() && mcresult.getCount() >= 1) {
								do {
									int keyIndex = mcresult.getColumnIndex(KEY_FIELD);
									int valueIndex = mcresult.getColumnIndex(VALUE_FIELD);
									String returnKey = mcresult.getString(0);
									String returnValue = mcresult.getString(valueIndex);
									String queryresult = "StarQuery:Key-".concat(returnKey).concat(";Value-").concat(returnValue);
									curresult.add(queryresult);
									Log.i("QUERY * RES", queryresult);
								}while(mcresult.moveToNext());
								mcresult.close();
							}
							//Ref: https://stackoverflow.com/questions/12895450/sending-an-arrayliststring-from-the-server-side-to-the-client-side-over-tcp-us
							if(mcresult.getCount()>0) {
								ObjectOutputStream oos = new ObjectOutputStream(client.getOutputStream());
								oos.writeInt(mcresult.getCount());
								oos.writeObject(curresult);
								oos.flush();
							}else
							{
								ObjectOutputStream oos = new ObjectOutputStream(client.getOutputStream());
								oos.writeInt(mcresult.getCount());
								oos.flush();
							}
						}else
						{
							Log.e(TAG,"CURSOR NULL");
						}

					}else if(message.contains("QUERY:"))
					{
						String key=message.split("Key-")[1].split(";")[0];
						Cursor mcresult=queryFile(key);
						if(mcresult!=null)
						{
							Log.i("MCresult",Integer.toString(mcresult.getCount()));

							if(mcresult.moveToFirst()) {
								int keyIndex = mcresult.getColumnIndex(KEY_FIELD);
								int valueIndex = mcresult.getColumnIndex(VALUE_FIELD);
								String returnKey = mcresult.getString(0);
								String returnValue = mcresult.getString(valueIndex);

								String queryresult = "ResultQuery:Key-".concat(returnKey).concat(";Value-").concat(returnValue);

								Log.i("QUERY RES",queryresult);

								PrintWriter outserver = new PrintWriter(client.getOutputStream(),true);
								outserver.println(queryresult);

								mcresult.close();
							}
						}


					}else if(message.contains("Delete*"))
					{	delete(mUri,"@",null);
						PrintWriter outack= new PrintWriter(client.getOutputStream(), true);
						outack.println("ACK");
					}else if(message.contains("Delete:"))
					{
						String filename=message.split("Key-")[1].split(";")[0];
						//delete(mUri,filename,null);
						getContext().deleteFile(filename);
						PrintWriter outack= new PrintWriter(client.getOutputStream(), true);
						outack.println("ACK");
					}else if(message.contains("RECOVER:PRED"))
					{
						Cursor mcresult=queryrecoverfiles();
						if(mcresult!=null)
						{
							Log.i("MCresult",Integer.toString(mcresult.getCount()));
							List<String> curresult=new ArrayList<String>();

							//Ref: https://stackoverflow.com/questions/4396604/how-to-solve-cursorindexoutofboundsexception
							if (mcresult.moveToNext() && mcresult.getCount() >= 1) {
								do {
									int keyIndex = mcresult.getColumnIndex(KEY_FIELD);
									int valueIndex = mcresult.getColumnIndex(VALUE_FIELD);
									String returnKey = mcresult.getString(0);
									String returnValue = mcresult.getString(valueIndex);
									String idhash = genHash(returnKey);
									String predhash=genHash(EMUL_PORTS.get(Integer.toString(predecessor_port)));
									String myhash=genHash(EMUL_PORTS.get(Integer.toString(my_port)));
									if ((idhash.compareToIgnoreCase(predhash) > 0 && idhash.compareToIgnoreCase(myhash) <= 0)|| (idhash.compareToIgnoreCase(predhash)>0 && isHead) || (idhash.compareToIgnoreCase(predhash)<0 && idhash.compareToIgnoreCase(myhash)<=0 && isHead)) {
										String queryresult = "RecoverQuery:Key-".concat(returnKey).concat(";Value-").concat(returnValue);
										curresult.add(queryresult);
										Log.i("QUERY * RES", queryresult);
									}
								}while(mcresult.moveToNext());
								mcresult.close();
							}
							//Ref: https://stackoverflow.com/questions/12895450/sending-an-arrayliststring-from-the-server-side-to-the-client-side-over-tcp-us
							if(mcresult.getCount()>0 && curresult.size()>0) {
								ObjectOutputStream oos = new ObjectOutputStream(client.getOutputStream());
								oos.writeInt(mcresult.getCount());
								oos.writeObject(curresult);
								oos.flush();
							}else
							{
								ObjectOutputStream oos = new ObjectOutputStream(client.getOutputStream());
								oos.writeInt(curresult.size());
								oos.flush();
							}
						}else
						{
							Log.e(TAG,"CURSOR NULL");
						}
					}else if(message.contains("RECOVER:SUCC"))
					{
						int predport=Integer.parseInt(message.split(";Origin;")[1]);
						int predpredport=0;
						Boolean predhead=false;
						for(int i=0;i<ringports.size();i++)
						{
							if(predport==ringports.get(i))
							{
								if(i==0)
								{
									predpredport=ringports.get(ringports.size()-1);
									predhead=true;

								}else
								{
									predpredport=ringports.get(i-1);
								}
							}
						}
						Cursor mcresult=queryrecoverfiles();
						if(mcresult!=null)
						{
							Log.i("MCresult",Integer.toString(mcresult.getCount()));
							List<String> curresult=new ArrayList<String>();

							//Ref: https://stackoverflow.com/questions/4396604/how-to-solve-cursorindexoutofboundsexception
							if (mcresult.moveToNext() && mcresult.getCount() >= 1) {
								do {
									int keyIndex = mcresult.getColumnIndex(KEY_FIELD);
									int valueIndex = mcresult.getColumnIndex(VALUE_FIELD);
									String returnKey = mcresult.getString(0);
									String returnValue = mcresult.getString(valueIndex);
									String idhash = genHash(returnKey);
									String predhash=genHash(EMUL_PORTS.get(Integer.toString(predpredport)));
									String myhash=genHash(EMUL_PORTS.get(Integer.toString(predport)));
									if ((idhash.compareToIgnoreCase(predhash) > 0 && idhash.compareToIgnoreCase(myhash) <= 0)|| (idhash.compareToIgnoreCase(predhash)>0 && predhead) || (idhash.compareToIgnoreCase(predhash)<0 && idhash.compareToIgnoreCase(myhash)<=0 && predhead)) {
										String queryresult = "RecoverQuery:Key-".concat(returnKey).concat(";Value-").concat(returnValue);
										curresult.add(queryresult);
										Log.i("QUERY * RES", queryresult);
									}
								}while(mcresult.moveToNext());
								mcresult.close();
							}
							//Ref: https://stackoverflow.com/questions/12895450/sending-an-arrayliststring-from-the-server-side-to-the-client-side-over-tcp-us
							if(mcresult.getCount()>0 && curresult.size()>0) {
								ObjectOutputStream oos = new ObjectOutputStream(client.getOutputStream());
								oos.writeInt(mcresult.getCount());
								oos.writeObject(curresult);
								oos.flush();
							}else
							{
								ObjectOutputStream oos = new ObjectOutputStream(client.getOutputStream());
								oos.writeInt(curresult.size());
								oos.flush();
							}
						}else
						{
							Log.e(TAG,"CURSOR NULL");
						}
					}

					client.close();
				}
			} catch (Exception e) {
				Log.e(TAG, "Server couldn't accept a Client");
				Log.e(TAG, e.toString());
			}


			return null;
		}


	}

	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {

			String remoteport = msgs[0];
			String msgToSend=msgs[1];
			try {

				Socket socket = new Socket();
				socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
				Integer.parseInt(remoteport)),4000);
				//Sending the message to the receivers
				PrintWriter output0 = new PrintWriter(socket.getOutputStream(), true);
				output0.println(msgToSend);

				//Close socket if ack received from server
				try {

					socket.setSoTimeout(2500);
				BufferedReader inserver = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				String msgRcvd = inserver.readLine();
				if(msgRcvd.contains("ACK"))
				{
					socket.close();
				}
				} catch (SocketTimeoutException e) {
					Log.e("Socket Timeout excep 1", e.toString());
					Log.e("Socket time", remoteport);
					socket.close();

				} catch (IOException e) {
					Log.e("IO Exception", e.toString());
					socket.close();

				}catch (NullPointerException e) {
					Log.e("Null Pointer Exception", e.toString());
					socket.close();
				}


			} catch (SocketTimeoutException e) {
				Log.e("Socket Timeout excep 2", e.toString());
				Log.e("Socket time", remoteport);

			} catch (IOException e) {

				Log.e("Socket IOexception 2", e.toString());
				Log.e("Socket time", remoteport);

			}

			return null;
		}

	}

	private class QueryTask extends AsyncTask<String, Void, String> {

		//Ref: https://stackoverflow.com/questions/14045765/waiting-for-asynctask-to-finish-or-variable-to-be-set
		@Override
		protected String doInBackground(String... msgs) {

			String remoteport = msgs[0];
			String msgToSend=msgs[1];
			try {

				Socket socket = new Socket();
				socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
				Integer.parseInt(remoteport)),4000);
				//Sending the message to the receivers
				PrintWriter output0 = new PrintWriter(socket.getOutputStream(), true);
				output0.println(msgToSend);
				//Thread.sleep(200);
				//Close socket if ack received from server
				try {

				socket.setSoTimeout(2500);
				BufferedReader inserver = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				String msgRcvd = inserver.readLine();
				if(msgRcvd.contains("ResultQuery:"))
				{
					Log.i("RESULT",msgRcvd);
					socket.close();
					return msgRcvd;

				}
				} catch (SocketTimeoutException e) {
					Log.e("Socket Timeout excep 1", e.toString());
					Log.e("Socket time", remoteport);
					socket.close();

				} catch (IOException e) {
					Log.e("IO Exception", e.toString());
					socket.close();

				}catch (NullPointerException e) {
					Log.e("Null Pointer Exception", e.toString());
					socket.close();
				}

			} catch (SocketTimeoutException e) {
				Log.e("Socket Timeout excep 2", e.toString());
				Log.e("Socket time", remoteport);

			} catch (IOException e) {

				Log.e("Socket IOexception 2", e.toString());
				Log.e("Socket time", remoteport);

			}

			return null;
		}


	}

	private class QueryStarTask extends AsyncTask<String, Void, List<String>> {

		//Ref: https://stackoverflow.com/questions/14045765/waiting-for-asynctask-to-finish-or-variable-to-be-set
		@Override
		protected List<String> doInBackground(String... msgs) {

			String remoteport = msgs[0];
			String msgToSend=msgs[1];
			try {

				Socket socket = new Socket();
				socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
				Integer.parseInt(remoteport)),4000);
				//Sending the message to the receivers
				PrintWriter output0 = new PrintWriter(socket.getOutputStream(), true);
				output0.println(msgToSend);

			try {

					socket.setSoTimeout(2500);
				//Ref: https://stackoverflow.com/questions/12895450/sending-an-arrayliststring-from-the-server-side-to-the-client-side-over-tcp-us
				ObjectInputStream oos = new ObjectInputStream(socket.getInputStream());
				int count=oos.readInt();
				List<String> reslist =null;
				if(count>0) {

					Object resobject = oos.readObject();
					reslist = (ArrayList<String>) resobject;
				}else{
					reslist =null;
				}
				socket.close();
				return reslist;
				} catch (SocketTimeoutException e) {
					Log.e("Socket Timeout excep 1", e.toString());
					Log.e("Socket time", remoteport);
					socket.close();

				} catch (IOException e) {
					Log.e("IO Exception", e.toString());
					socket.close();

				}catch (NullPointerException e) {
					Log.e("Null Pointer Exception", e.toString());
					socket.close();
				}
				catch(ClassNotFoundException e)
				{
					Log.e("ClassNotFound Exception",e.toString());
					socket.close();
				}

			} catch (SocketTimeoutException e) {
				Log.e("Socket Timeout excep 2", e.toString());
				Log.e("Socket time", remoteport);

			} catch (IOException e) {

				Log.e("Socket IOexception 2", e.toString());
				Log.e("Socket time", remoteport);

			}
			return null;
		}


	}
}
