package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity {
	static final String TAG = SimpleDynamoActivity.class.getName();
	public static final String LDumpSelection = "@";
	public static final String GDumpSelection = "*";
	public static final String KEY_FIELD = "key";
	public static final String VALUE_FIELD = "value";
	public static final String PORT = "port";
	public static SimpleDynamoActivity singleActivity;

	public static Uri contentURI;
	public static TextView tv;
	public Cursor resultCursor;

	public static SQLHelperClass sql;
	/*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/
	public static String INSERT   = "INSERT"; //insert the data + asks for replication
	public static String REPLICATE= "REPLICATE";  //does the replication
	public static String QUERY_LOOKUP = "QUERY_LOOKUP";  // used by originator for both "@" and "*" query
	/*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/
	public static SimpleDynamoActivity getInstance() {
		return singleActivity;
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
		contentURI = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
		sql = SQLHelperClass.getInstance(getApplicationContext());

		tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

		final TextView editText = (TextView) findViewById(R.id.editText);

		findViewById(R.id.btnSend).setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View v) {
				ContentValues cv = new ContentValues();
				cv.put(KEY_FIELD, editText.getText().toString());

				cv.put(VALUE_FIELD, "value" + editText.getText().toString().charAt(editText.getText().length() - 1));
				editText.setText("");

				Uri uri = getContentResolver().insert(contentURI, cv);
				if (uri == null) {
					Log.e(TAG, "insert Failed for btnSend");
				}

			}
		});
		if (singleActivity == null)
			singleActivity = this;
	}


	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");
	}

	public void setText(final String value) {
		runOnUiThread(new Runnable() {
			@Override
			public void run() {
				tv.append(value);
				tv.append("\n************\n");
			}
		});
	}

}
