package dao;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import com.mysql.jdbc.Util;

import utility.UtilityHelper;
import logger.AggregationLogger;
import main.AqiCalculationIndia;
import main.DataAggregation;
import main.DataAggregation.table;

/**
 * 
 * @author Gaurav
 *
 **/

public class AggregateDao extends BaseDao {
	
	public static AggregateDao aggrdao = null;
	
	String SELECT_LOCATION_ID = "Select distinct(location_id) from pollution_data_temp";
	String SELECT_COUNTRY = "Select country from location where id = ?";
	String SELECT_PARAM = "Select pollutant_name from pollution_params where country = ?";
	String INSERT_FROM_TEMP_TO_RAW = "INSERT INTO pollution_data_raw SELECT * FROM pollution_data_temp WHERE location_id = ? AND insert_datetime < ?";
	String DELETE_AGGREGATE_DATA_FROM_TEMP = "DELETE FROM pollution_data_temp WHERE location_id = ? AND insert_datetime < ?";
	
	AggregationLogger logger = new AggregationLogger();
	
	public ResultSet selectLocationId()
	{
		ResultSet rs = null;
		try
		{
			ArrayList<HashMap<fieldType, Object>> queryRawData = UtilityHelper.prepareDaoDefaultArgumentsObject();
			rs = getQueryResult(SELECT_LOCATION_ID, queryRawData, "select");
		}
		catch(Exception e)
		{
			e.printStackTrace();
			logger.error(this, e.toString());
		}
		return rs;
	}
	
	public ResultSet getCountryName(int location_id)
	{
		ResultSet rs = null;
		try
		{
			ArrayList<HashMap<fieldType, Object>> queryRawData = UtilityHelper.prepareDaoDefaultArgumentsObject();
			queryRawData = UtilityHelper.addValueInDaoArguments(BaseDao.fieldType.Int, location_id, queryRawData);

			rs = getQueryResult(SELECT_COUNTRY, queryRawData, "select");
		}
		catch(Exception e)
		{
			e.printStackTrace();
			logger.error(this, e.toString());
		}
		return rs;
	}
	
	public ResultSet getParamName(String country)
	{
		ResultSet rs = null;
		try
		{
			ArrayList<HashMap<fieldType, Object>> queryRawData = UtilityHelper.prepareDaoDefaultArgumentsObject();
			queryRawData = UtilityHelper.addValueInDaoArguments(BaseDao.fieldType.String, country, queryRawData);

			rs = getQueryResult(SELECT_PARAM, queryRawData, "select");
		}
		catch(Exception e)
		{
			e.printStackTrace();
			logger.error(this, e.toString());
		}
		return rs;
	}
	
	public Timestamp getAggregationDateTime(int time_duration)
	{
		ResultSet rs = null;
		try(CallableStatement stmt = new BaseDao().DBConn.getConnection().prepareCall("{call getAggregateDateTime(?)}");) {
			
			stmt.setInt(1, time_duration);
			rs = stmt.executeQuery();
			
			rs.next();
			Timestamp datetime = rs.getTimestamp("aggregation_time");
			return datetime;
		}catch(SQLException e){
			e.printStackTrace();
			logger.error(this, e.toString());
			return null;
		}
		
	}
	
	public void runAggregationFunction(ResultSet rs1, int location_id, int time_duration, Timestamp datetime, String country)
	{
		ResultSet rs = null;
		try(CallableStatement statement = new DBConnection().getConnection().prepareCall("{call sp_pmiaggregate(?, ?, ?)}");){
 
			statement.setInt(1, location_id);
			statement.setInt(2, time_duration);
			statement.setTimestamp(3, datetime);
			
            rs = statement.executeQuery();
            
            HashMap<String, Object> dataset = new HashMap<String, Object>();
		    dataset.put("data_datetime", new Object());
		    dataset.put("insert_datetime", new Object());
		    while(rs1.next()) {
		    	dataset.put(rs1.getString("pollutant_name"),new Object());
		    }
		while(rs.next()) {
			int check_pm =0, count_param =0;
			Object[] maxAqi = new Object[2];
	    	for(String key : dataset.keySet()){
	    		if(key.trim().equalsIgnoreCase("insert_datetime")){
	    			dataset.put(key, new Timestamp(new Date().getTime()));
	    		}
	    		else if(key.trim().equalsIgnoreCase("data_datetime")) {
	    			dataset.put(key, rs.getTimestamp("data_datetime"));
	    		}
	    		else if(key.trim().equalsIgnoreCase("location_id")){
	    			continue;
	    		}
	    		else if(key.trim().equalsIgnoreCase("aqi_contributor")){
	    			continue;
	    		}
	    		else if(key.trim().equalsIgnoreCase("aqi_val")){
	    			continue;
	    		}
	    		else if(key.trim().equalsIgnoreCase("status")){
	    			continue;
	    		} 
	    		else{
	    			dataset.put(key, rs.getDouble(key));
	    			if(check_pm == 0 && key.trim().equalsIgnoreCase("pm25") || key.trim().equalsIgnoreCase("pm10")) {
	    				check_pm = 1;
	    			}
	    			count_param++;
	    		}
	    	}
	    	dataset.put("location_id", rs.getInt("location_id"));
	    	if(check_pm == 1 && count_param >=3){
	    		if(country.equalsIgnoreCase("india")){
	    			maxAqi = AqiCalculationIndia.aqi_calculation(dataset);
	    		}
	    	}
	    	dataset.put(table.aqi_contributor.toString(), maxAqi[0].toString());
	    	dataset.put(table.aqi_val.toString(), (double)maxAqi[1]);
	    	dataset.put(table.status.toString(), (int)1);
	    	int id = insertPollutionAggregatedData(dataset, table.pollution_data.toString(),table.id.toString());
	    }
		statement.close();
        }catch(SQLException e)
		{
			e.printStackTrace();
			logger.error(this, e.toString());
		}
	}
	
	public int insertPollutionAggregatedData(HashMap<String, Object> dataset, String tableName, String primaryColumnNameInTable)
	{
		ArrayList<HashMap<fieldType, Object>> queryRawData = UtilityHelper.prepareDaoDefaultArgumentsObject();
		ArrayList<String> listOfColoumName = new ArrayList<>();
		for(String colName: dataset.keySet()){
			if(!colName.equalsIgnoreCase(primaryColumnNameInTable)){
				Object obj =  dataset.get(colName);
				listOfColoumName.add(colName);
				if(obj instanceof  Integer){ queryRawData = UtilityHelper.addValueInDaoArguments(BaseDao.fieldType.Int, obj, queryRawData);}
				if(obj instanceof  String){  queryRawData = UtilityHelper.addValueInDaoArguments(BaseDao.fieldType.String, obj, queryRawData);}
				if(obj instanceof  Double){   queryRawData = UtilityHelper.addValueInDaoArguments(BaseDao.fieldType.Double, obj, queryRawData);}
				if(obj instanceof  Timestamp){ 		queryRawData = UtilityHelper.addValueInDaoArguments(BaseDao.fieldType.Timestamp, obj, queryRawData);}
			}
		}
		int id = insertQuery(queryRawData, listOfColoumName, tableName, primaryColumnNameInTable);
		return id;
	}
	
	public int insertDataFromTempToRaw(Timestamp datetime, int location_id)
	{
		int id = 0;
		ResultSet rs = null;
		try
		{
			ArrayList<HashMap<fieldType, Object>> queryRawData = UtilityHelper.prepareDaoDefaultArgumentsObject();
			queryRawData = UtilityHelper.addValueInDaoArguments(BaseDao.fieldType.Int, location_id, queryRawData);
			queryRawData = UtilityHelper.addValueInDaoArguments(BaseDao.fieldType.Timestamp, datetime, queryRawData);

			rs = getQueryResult(INSERT_FROM_TEMP_TO_RAW, queryRawData, "insert");
		}
		catch(Exception e)
		{
			e.printStackTrace();
			logger.error(this, e.toString());
		}
		return id;
	}
	
	public ResultSet deleteAggregateDataFromTemp(Timestamp datetime, int location_id) {
		ResultSet rs = null;
		try{
			ArrayList<HashMap<fieldType, Object>> queryRawData = UtilityHelper.prepareDaoDefaultArgumentsObject();
			queryRawData = UtilityHelper.addValueInDaoArguments(BaseDao.fieldType.Int, location_id, queryRawData);
			queryRawData = UtilityHelper.addValueInDaoArguments(BaseDao.fieldType.Timestamp, datetime, queryRawData);

			
			rs = getQueryResult(DELETE_AGGREGATE_DATA_FROM_TEMP, queryRawData, "delete");
		}catch(Exception e){
			e.printStackTrace();
			logger.error(this, e.toString());
		}
		
		return rs;
	}
	
	public static AggregateDao getInstance()
	{
		if(aggrdao == null)
			aggrdao = new AggregateDao();
		return aggrdao;
	}
}