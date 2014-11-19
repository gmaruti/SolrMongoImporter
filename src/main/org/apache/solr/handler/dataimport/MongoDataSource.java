package org.apache.solr.handler.dataimport;

import com.mongodb.*;
import com.mongodb.util.JSON;

import org.apache.solr.handler.dataimport.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;

/**
 * User: James Date: 13/08/12 Time: 18:28 To change this template use File |
 * Settings | File Templates.
 */

public class MongoDataSource extends DataSource<Iterator<Map<String, Object>>> {

	private static final Logger LOG = LoggerFactory
			.getLogger(MongoDataSource.class);

	private DBCollection mongoCollection;
	private DB mongoDb;
	private Mongo mongo;
	private DBCursor mongoCursor;
	private int batchSize = 500;

	@Override
	public void init(Context context, Properties initProps) {
		String databaseName = initProps.getProperty(DATABASE);
		String host = initProps.getProperty(HOST, "localhost");
		String port = initProps.getProperty(PORT, "27017");
		String username = initProps.getProperty(USERNAME);
		String password = initProps.getProperty(PASSWORD);
		String batchSizeStr = initProps.getProperty(BATCH_SIZE);
		if (batchSizeStr != null)
		{
			try
			{
				batchSize = Integer.parseInt(batchSizeStr);
			}
			catch (Exception e)
			{
				LOG.warn("Exception in parsing batchSize ", e);
			}
				
		}
		if (databaseName == null) {
			throw new DataImportHandlerException(SEVERE,
					"Database must be supplied");
		}

		try {
			this.mongo = new Mongo(host, Integer.parseInt(port));
			
			this.mongoDb = mongo.getDB(databaseName);

			if (username != null) {
				if (this.mongoDb.authenticate(username, password.toCharArray()) == false) {
					throw new DataImportHandlerException(SEVERE,
							"Mongo Authentication Failed");
				}
			}
		} catch (UnknownHostException e) {
			throw new DataImportHandlerException(SEVERE,
					"Unable to connect to Mongo");
		}
	}

	@Override
	public Iterator<Map<String, Object>> getData(String query) {


		DBObject queryObject = (DBObject) JSON.parse(query.toString());


		long start = System.currentTimeMillis();
		int batchSize = 500;
		int fetchedResult = 0;
		
		mongoCursor = this.mongoCollection.find(queryObject).limit(batchSize);
		ResultSetIterator resultSet = new ResultSetIterator(mongoCursor);
		Iterator<Map<String, Object>> itr = resultSet.getIterator();
		
		while (mongoCursor.size() == batchSize){
			mongoCursor = this.mongoCollection.find(queryObject).limit(batchSize).skip(fetchedResult + batchSize);
			resultSet = new ResultSetIterator(mongoCursor);
			itr = resultSet.getIterator();
			
		}
		
		return resultSet.getIterator();
	}

	public Iterator<Map<String, Object>> getData(String query, String collection) {
		this.mongoCollection = this.mongoDb.getCollection(collection);
		return getData(query);
	}

	private class ResultSetIterator {
		DBCursor MongoCursor;

		Iterator<Map<String, Object>> rSetIterator;

		public ResultSetIterator(DBCursor MongoCursor) {
			this.MongoCursor = MongoCursor;

			rSetIterator = new Iterator<Map<String, Object>>() {
				public boolean hasNext() {
					return hasnext();
				}

				public Map<String, Object> next() {
					return getARow();
				}

				public void remove() {/* do nothing */
				}
			};

		}

		public Iterator<Map<String, Object>> getIterator() {
			return rSetIterator;
		}

		private Map<String, Object> getARow() {
			DBObject mongoObject = getMongoCursor().next();

			Map<String, Object> result = new HashMap<String, Object>();
			Set<String> keys = mongoObject.keySet();
			Iterator<String> iterator = keys.iterator();

			while (iterator.hasNext()) {
				String key = iterator.next();
				Object innerObject = mongoObject.get(key);
				result.put(key, innerObject);
			}

			return result;
		}

		private boolean hasnext() {
			if (MongoCursor == null)
				return false;
			try {
				if (MongoCursor.hasNext()) {
					return true;
				} else {
					close();
					return false;
				}
			} catch (MongoException e) {
				close();
				wrapAndThrow(SEVERE, e);
				return false;
			}
		}

		private void close() {
			try {
				if (MongoCursor != null)
					MongoCursor.close();
			} catch (Exception e) {
				LOG.warn("Exception while closing result set", e);
			} finally {
				MongoCursor = null;
			}
		}
	}

	private DBCursor getMongoCursor() {
		return this.mongoCursor;
	}

	@Override
	public void close() {
		if (this.mongoCursor != null) {
			this.mongoCursor.close();
		}
		
		if (this.mongo != null) {
			this.mongo.close();
		}
	}

	public static final String DATABASE = "database";
	public static final String HOST = "host";
	public static final String PORT = "port";
	public static final String USERNAME = "username";
	public static final String PASSWORD = "password";
	public static final String BATCH_SIZE = "batchSize";
}
