package dao;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;

import utility.Utility;
import utility.UtilityHelper;
import logger.CrawlLogger;

/**
 * 
 * @author Gaurav
 *
 **/

public class CrawlDao extends BaseDao {
	
	public static CrawlDao crawldao = null;
	
	String SELECT_LOCATION = "Select * from location where source_id=1 and status=0";
	String SELECT_PARAM_UNIT = "Select pollutant_name, reading_type from pollution_params where country = ?";
	
	CrawlLogger logger = new CrawlLogger();
	
	public ResultSet getLocationData()
	{
		ResultSet rs = null;
		try
		{
			ArrayList<HashMap<fieldType, Object>> queryRawData = UtilityHelper.prepareDaoDefaultArgumentsObject();
			rs = getQueryResult(SELECT_LOCATION, queryRawData, "select");
		}
		catch(Exception e)
		{
			e.printStackTrace();
			logger.error(this, e.toString());
		}
		return rs;
	}
	
	public ResultSet getParamUnit(String country)
	{
		ResultSet rs = null;
		try
		{
			ArrayList<HashMap<fieldType, Object>> queryRawData = UtilityHelper.prepareDaoDefaultArgumentsObject();
			queryRawData = UtilityHelper.addValueInDaoArguments(BaseDao.fieldType.String, country, queryRawData);
			rs = getQueryResult(SELECT_PARAM_UNIT, queryRawData, "select");
		}
		catch(Exception e)
		{
			e.printStackTrace();
			logger.error(this, e.toString());
		}
		return rs;
	}
	
	public int insertPollutionData(HashMap<String, Object> dataset, String tableName, String primaryColumnNameInTable)
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
	
	public ResultSet updateLastCrawl(HashMap<String, Object> dataset, String tableName, String primaryColumName)
	{
		ArrayList<HashMap<fieldType, Object>> queryRawData = UtilityHelper.prepareDaoDefaultArgumentsObject();
		ArrayList<String> listOfColoumName = new ArrayList<>();
		for(String colName: dataset.keySet()){
			if(colName.trim().equalsIgnoreCase(primaryColumName)){
				continue;
			}
			Object obj =  dataset.get(colName);
			listOfColoumName.add(colName);
			if(obj instanceof  Double){ queryRawData = UtilityHelper.addValueInDaoArguments(BaseDao.fieldType.Double, obj, queryRawData);}
			if(obj instanceof  Integer){ queryRawData = UtilityHelper.addValueInDaoArguments(BaseDao.fieldType.Int, obj, queryRawData);}
			if(obj instanceof  String){  queryRawData = UtilityHelper.addValueInDaoArguments(BaseDao.fieldType.String, obj, queryRawData);}
		}
		listOfColoumName.add(primaryColumName);
		queryRawData = UtilityHelper.addValueInDaoArguments(BaseDao.fieldType.Int, dataset.get(primaryColumName), queryRawData);

		ResultSet rs = updateQuery(queryRawData, listOfColoumName, tableName);
		return rs;
	}
	
	public static CrawlDao getInstance()
	{
		if(crawldao == null)
			crawldao = new CrawlDao();
		return crawldao;
	}
}