package dao;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import logger.CrawlLogger;
import utility.Utility;
import utility.UtilityHelper;

/**
 * 
 * @author Gaurav
 *
 */
public class BaseDao  {
	
	CrawlLogger logger = new CrawlLogger();
	DBConnection DBConn = new DBConnection();
	public String queryBeginKey = "{{";
	public String queryEndKey = "}}";

	private Connection conn = null;
	private PreparedStatement ps = null;
	private ResultSet rs = null;

	/**
	 * 
	 *
	 */
	public static enum fieldType {
		Int, Long, Float, Double, Date, Timestamp, Boolean, Byte, String, Character;

		public String getName() {
			return name();

		}
	}

	public ResultSet getQueryResult(String queryString, ArrayList<HashMap<fieldType, Object>> arguments, String actionName) {
		return getQueryResult(queryString, arguments, actionName, null);
	}

	/**
	 * This method prepare query string base on arguments and action name
	 * 
	 * @param queryString
	 *          : query parameters
	 * @param arguments
	 *          : LinkedHashMap<fieldType,Object>
	 * @param actionName
	 *          : actionName (select , insert , update , delete)
	 * @return ResultSet
	 * @throws ServerApplicationException
	 *           SELECT FROM TABLE WHERE COLOMUN NAME = {ABC} INT <ABC TODO In
	 *           future column will be access from column name right now it access
	 *           from index number
	 */
	private synchronized ResultSet getQueryResult(String queryString, ArrayList<HashMap<fieldType, Object>> arguments, String actionName, String primaryKeyColoumName) {
		this.ps = null;
		this.rs = null;
		this.conn = null;
		int counter = 1;
		try {
			this.conn = DBConn.getConnection() ;
			if (primaryKeyColoumName == null) {
				ps = conn.prepareStatement(queryString);
			} else {
				//String keyGeneratingColumn[] = { primaryKeyColoumName };
				ps = conn.prepareStatement(queryString, Statement.RETURN_GENERATED_KEYS);
			}

			for (int i = 0; i < arguments.size(); i++) {
				HashMap<fieldType, Object> argumentsinfo = arguments.get(i);
				for (Map.Entry<fieldType, Object> entry : argumentsinfo.entrySet()) {
					BaseDao.fieldType selectedIdentity = entry.getKey();
					Object queryvalue = entry.getValue();
					switch (selectedIdentity) {
					case Int:
						ps.setInt(counter, ((Integer) queryvalue).intValue());
						break;
						
					case Long:
						ps.setLong(counter, ((Long) queryvalue).longValue());
						break;

					case Float:
						ps.setFloat(counter, ((Float) queryvalue).floatValue());
						break;

					case Double:
						ps.setDouble(counter, ((Double) queryvalue).doubleValue());
						break;

					case Boolean:
						ps.setBoolean(counter, ((Boolean) queryvalue).booleanValue());
						break;

					case String:
						ps.setString(counter, ((String) queryvalue).toString());
						break;

					case Timestamp:
//						Calendar objClass = (Calendar) queryvalue;
//						Timestamp lastupdateTimestamp = new Timestamp(objClass.getTimeInMillis());
						ps.setTimestamp(counter, ((Timestamp) queryvalue));
						break;
						
					case Date:
						Date objDate = (Date) queryvalue;
						ps.setDate(counter, objDate);
						break;
					default:
						break;
					}
					counter = counter + 1;
				}
			}
			ps.getParameterMetaData();
			ps.execute();
			rs = (actionName.equalsIgnoreCase("insert")) ? ps.getGeneratedKeys() : ps.getResultSet();
		} catch (SQLException e) {
			logger.error(this, " :SQLException occured ", e);
			e.printStackTrace();
		} finally {
//			logger.debug(this, "getQueryResult() query execute finished");
		}
		return rs;
	}

	/**
	 * This method will to geT value from column Name
	 * 
	 * @param rs
	 * @param filedName
	 * @param fieldType
	 * @param defaultValue
	 * @return
	 */
	public ResultSet updateQuery(ArrayList<HashMap<fieldType, Object>> arguments, ArrayList<String> listOfColoumName, String tableName)   {
		StringBuilder updateQuery = new StringBuilder();
		updateQuery.append("UPDATE "+ tableName +" SET ");
		int i;
		for(i = 0 ; i < listOfColoumName.size()-1; i++){
			if(i > 0){updateQuery.append(", ");}
			updateQuery.append(listOfColoumName.get(i));
			updateQuery.append(" = ? ");
		}
		updateQuery.append(" WHERE " +listOfColoumName.get(i) +" = ?");
		ResultSet result = null;
			result = this.getQueryResult(updateQuery.toString(), arguments, "update");
			return result;
	}

	/**
	 * This method is used to insert data in data Prepare query base on arguments
	 * and listOfColoumName required filed tableName If(primeryColounNameInTable
	 * is null then insert query method return 0) else is return value last insert
	 * value of column primeryColounNameInTable
	 * 
	 * @param arguments
	 *          : arguments base on argument type and value
	 * @param listOfColoumName
	 *          : list of ColounName
	 * @param tableName
	 * @param primaryColumnNameInTable
	 * @return
	 * @ 
	 *           abc
	 * 
	 */
	public int insertQuery(ArrayList<HashMap<fieldType, Object>> arguments, ArrayList<String> listOfColoumName, String tableName, String primaryColumnNameInTable)   {

		if (arguments == null || listOfColoumName == null || arguments.size() != listOfColoumName.size()) {
			logger.error(this, "insertQuery() listOfColoumName size is not equal to arguments");
			String[] exceptionParams = { "" };
		}

		// prepare insert query
		StringBuilder query = new StringBuilder();
		query.append("INSERT INTO " + tableName + "(");
		StringBuilder valuePlaceHolder = new StringBuilder();

		for (int i = 0; i < listOfColoumName.size(); i++) {
			if (i > 0) {
				query.append(",");
				valuePlaceHolder.append(",");
			}
			query.append(" " + listOfColoumName.get(i) + " ");
			valuePlaceHolder.append("?");
		}
		query.append(" )  VALUES (" + valuePlaceHolder.toString() + "  )");
		
		int resultValue = 0;
		ResultSet result = null;

		try {
			if (primaryColumnNameInTable != null) {
				result = this.getQueryResult(query.toString(), arguments, "insert", primaryColumnNameInTable);
				if (result.next()) {
					resultValue = result.getInt(1);
				}
			} else {
				result = this.getQueryResult(query.toString(), arguments, "insert");

			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(this, e.toString());
		} 

		return resultValue;
	}
	
	
	public int multipleInsertQuery(ArrayList<HashMap<fieldType, Object>> arguments, ArrayList<String> listOfColoumName, String tableName, String primaryColumnNameInTable, int row)   {

		if (arguments == null || listOfColoumName == null || arguments.size() != listOfColoumName.size()) {
			logger.error(this, "insertQuery() listOfColoumName size is not equal to arguments");
			String[] exceptionParams = { "" };
		}

		// prepare insert query
		StringBuilder query = new StringBuilder();
		query.append("INSERT INTO " + tableName + "(");
		StringBuilder valuePlaceHolder = new StringBuilder();

		for (int i = 0; i < listOfColoumName.size(); i++) {
			if (i > 0) {
				query.append(",");
				valuePlaceHolder.append(",");
			}
			query.append(" " + listOfColoumName.get(i) + " ");
			valuePlaceHolder.append("?");
		}
		//query.append(" )  VALUES (" + valuePlaceHolder.toString() + "  )");
		query.append(" ) VALUES ");
		for (int i =0; i < row; i++)
		{
			if(i > 0){
				query.append(",");
			}
			query.append("(" + valuePlaceHolder.toString() + " )");
		}

		int resultValue = 0;
		ResultSet result = null;

		try {
			if (primaryColumnNameInTable != null) {
				result = this.getQueryResult(query.toString(), arguments, "insert", primaryColumnNameInTable);
				if (result.next()) {
					resultValue = result.getInt(primaryColumnNameInTable);
				}
			} else {
				result = this.getQueryResult(query.toString(), arguments, "insert");

			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(this, e.toString());
		} 

		return resultValue;
	}

	/**
	 * To run Insert query
	 * 
	 * @param query
	 *          : String
	 * @param arguments
	 *          : LinkedHashMap<fieldType,Object>
	 * @return : ResultSet
	 * @ 
	 *           insert into tablename (a,c,c) values (? , ? , ?)
	 */
	public ResultSet insertQuery(String query, ArrayList<HashMap<fieldType, Object>> arguments)   {
		ResultSet result = null;
		try {
			result = this.getQueryResult(query, arguments, "insert");
		} catch (Exception e) {
			logger.error(this, "insertQuery() Fail to insert data errorMessage:" + e.toString() );
			logger.error(this, "insertQuery() Query:" + query);
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * If You need to fetch insert id (primary key )
	 * 
	 * @param query
	 * @param arguments
	 * @param primeryColounNameInTable
	 *          : it should be primary key in table
	 * @return
	 * @ 
	 *
	 */
	public int insertQuery(String query, ArrayList<HashMap<fieldType, Object>> arguments, String primeryColounNameInTable)   {
		ResultSet result = null;
		int resultValue = 0;
		try {
			result = this.getQueryResult(query, arguments, "insert", primeryColounNameInTable);
		} catch (Exception e) {
			logger.error(this, "insertQuery() Fail to insert data errorMessage:" + e.getMessage() );
			logger.error(this, "insertQuery() Query:" + query);
			e.printStackTrace();
		} 
		
		try {
			if (rs.next()) {
				resultValue = result.getInt(primeryColounNameInTable);
			}
//			this.ps.close();
//			this.rs.close();
		} catch (SQLException e) {
			logger.error(this, "sql error message :" + e.getMessage());
			e.printStackTrace();
		}
		return resultValue;
	}



	/**
	 * If You need to fetch insert id (primary key )
	 * 
	 * @param <T>
	 * 
	 * @param query
	 * @param arguments
	 * @param primeryColounNameInTable
	 *          : it should be primary key in table
	 * @return
	 * @ 
	 *
	 */
	public <T> T getValue(String query, ArrayList<HashMap<fieldType, Object>> arguments, String fieldName, String primeryColounNameInTable, fieldType columnFieldType, T defaultValue)   {
		ResultSet result = null;
		T fieldVal = defaultValue;
		try {
			result = this.getQueryResult(query, arguments, "select", primeryColounNameInTable);
			fieldVal = getValue(result, fieldName, columnFieldType, defaultValue );
		} catch (Exception e) {
			logger.error(this, "Query:" + query +" failed to fetch the data" + e.toString());
			e.printStackTrace();
		}
		return fieldVal;
	}
	
	 /**
	   * This method will to geT value from column Name
	   * 
	   * @param rs
	   * @param fieldName
	   * @param fieldType
	   * @param defaultValue
	   * @return
	   */
		
	  public <T> T getValue(ResultSet rs, String fieldName, BaseDao.fieldType fieldType, T defaultValue) {
		T returnDefaultValue = defaultValue;
		Object returnValue = null;

		try {
		  switch (fieldType) {
		  case Int:

			if (rs.getInt(fieldName) > 0) {
			  returnValue = rs.getInt(fieldName);
			}
			break;
		  case Long:
			if (rs.getLong(fieldName) > 0) {
			  returnValue = rs.getLong(fieldName);
			}

			break;
		  case Float:
			if (rs.getFloat(fieldName) > 0) {
			  returnValue = rs.getFloat(fieldName);
			}
			break;

		  case Double:
			if (rs.getDouble(fieldName) > 0) {
			  returnValue = rs.getDouble(fieldName);
			}
			break;

		  case Boolean:
			returnValue = rs.getBoolean(fieldName);
			break;

		  case String:
			if (!UtilityHelper.isStringEmpty(rs.getString(fieldName))) {
			  returnValue = rs.getString(fieldName);
			}
			break;

		  case Timestamp:
			if (!UtilityHelper.isStringEmpty(rs.getTimestamp(fieldName).toString())) {
			  returnValue = (Utility.dateToCalanderObject(rs.getTimestamp(fieldName).toString()));
			}
			break;

		  default:
			break;
		  }
		} catch (SQLException e) {
			e.printStackTrace();
		  logger.debug(this, "Value cant convert to desire output : coloumName=" + fieldName + ",FieldType=" + fieldType.toString());
		  return returnDefaultValue;
		} catch (Exception e) {
		   e.printStackTrace();
		  logger.debug(this, "Value cant convert to desire output : coloumName=" + fieldName + ",FieldType=" + fieldType.toString());
		  
		}

		return returnDefaultValue;
	 }
	
	public void create_table_if_not_exists(String createTableQury) throws IOException {

		// create table scada_values if not exist already
		if (conn == null) {
			this.conn = DBConn.getConnection();
		}

		try {
			Statement st = conn.createStatement();
			st.executeUpdate(createTableQury);

		} catch (Exception e) {
			e.printStackTrace();
			String[] CreateQuery =  createTableQury.split("\\s");
			logger.error(this, "Exception while creating table  "+ CreateQuery[5].trim()+" error message :" + e.getMessage());
		}

	}
	public ResultSet deleteResult(String query, ArrayList<HashMap<fieldType, Object>> arguments)   {
		ResultSet result = null;
		try {
			result = this.getQueryResult(query, arguments, "delete");
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(this, "deleteResult() Fail to delete data errorMessage:" + e.getMessage() + ",errorCode+" + e.hashCode());
			logger.error(this, "deleteResult() Query:" + query);
		}
		return result;
	}
}