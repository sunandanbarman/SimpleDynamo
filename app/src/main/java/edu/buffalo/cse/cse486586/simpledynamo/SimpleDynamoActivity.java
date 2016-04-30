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
	public static final String KEY_FIELD   = "key";
	public static final String VALUE_FIELD = "value";
	public static final String PORT        = "port";
	public static final String REPLICATE   = "replicate";
	public static final String VERSION     = "version";
	public static SimpleDynamoActivity singleActivity;

	public static Uri contentURI;
	public static TextView tv;
	public Cursor resultCursor;


	/*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/
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
		//sql = SQLHelperClass.getInstance(getApplicationContext());

		tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

		final TextView editText = (TextView) findViewById(R.id.editText);
		/*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/
		findViewById(R.id.btnSend).setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View v) {
				if (editText.getText().toString().equalsIgnoreCase("")) {
					Log.e(TAG,"Please enter something");
					return;
				}

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
		findViewById(R.id.btnQueryText).setOnClickListener(
				new View.OnClickListener() {
					@Override
					public void onClick(View v) {

						resultCursor = getContentResolver().query(contentURI, null,
								editText.getText().toString(), null, null);
						if (resultCursor == null) {
							Log.e(TAG, "Data not found");
						}
						resultCursor.close();
					}
				});

		findViewById(R.id.btnClear).setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View v) {
				tv.setText("");
			}
		});
		findViewById(R.id.btnLDump).setOnClickListener(
				new View.OnClickListener() {
					@Override
					public void onClick(View v) {
						resultCursor = getContentResolver().query(contentURI, null,
								LDumpSelection, null, null);
						if (resultCursor == null) {
							Log.e(TAG, "Result null");

						}

						int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
						int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
						if (keyIndex == -1 || valueIndex == -1) {
							Log.e(TAG, "Wrong columns");
							resultCursor.close();

						}

						resultCursor.moveToFirst();
						tv.append("************LDump starts******\n");
						while (!resultCursor.isAfterLast()) {
							String key = resultCursor.getString(keyIndex);
							String val = resultCursor.getString(valueIndex);
							Log.e(TAG, " key :" + key + " value :" + val);
							tv.append(key + "=" + val);
							tv.append("\n");
							resultCursor.moveToNext();
						}
						tv.append("\n************LDump ends******");
						Log.e(TAG, "Found rows are :" + String.valueOf(resultCursor.getCount()));

						resultCursor.close();
					}
				});

		findViewById(R.id.btnGDump).setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View v) {
				resultCursor = getContentResolver().query(contentURI, null, GDumpSelection, null, null);
				if (resultCursor == null) {

					Log.e(TAG, "resultCursor null");
					return;
				}
				int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
				int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
				if (keyIndex == -1 || valueIndex == -1) {
					Log.e(TAG, "Wrong columns");
					resultCursor.close();

				}

				resultCursor.moveToFirst();
				tv.append("************GDump starts******\n");
				while (!resultCursor.isAfterLast()) {
					String key = resultCursor.getString(keyIndex);
					String val = resultCursor.getString(valueIndex);
					Log.e(TAG, " key :" + key + " value :" + val);
					tv.append(key + "=" + val);
					tv.append("\n");
					resultCursor.moveToNext();
				}
				tv.append("\n************GDump ends******");
				Log.e(TAG, "Found rows are :" + String.valueOf(resultCursor.getCount()));

				resultCursor.close();

				if (resultCursor != null)
					resultCursor.close();

			}
		});
		final TextView edtHash = (TextView) findViewById(R.id.editText);
		findViewById(R.id.btnGenHash).setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View v) {
				edtHash.getText().toString();
			}
		});
		/*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/
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
