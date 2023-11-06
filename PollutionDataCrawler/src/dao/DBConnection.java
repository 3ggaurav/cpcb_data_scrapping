package dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import configuration.Configurator;
import logger.CrawlLogger;


public class DBConnection {
	private DBConnection dbConn = null;
	static Connection conn = null;
	static CrawlLogger logger = new CrawlLogger();

	public Connection getConnection() {
		Configurator appConfig = Configurator.getInstance();
		String driver_type = appConfig.getProperty("driver_type");
		String db_type = appConfig.getProperty("db_type");
		String db_address = appConfig.getProperty("db_address");
		String db_name = appConfig.getProperty("db_name");
		String db_user = appConfig.getProperty("db_user");
		String db_password = appConfig.getProperty("db_password");
		String auto_reconnect = appConfig.getProperty("auto_reconnect");
		String use_ssl = appConfig.getProperty("use_ssl");

		String db_url = driver_type+":"+db_type+"://"+ db_address  +"/"+ db_name+ "?autoReconnect="+auto_reconnect+"&useSSL="+use_ssl+"";

		try {
			if (conn == null) {
					logger.info(this, "Connecting with Database...");
					conn = DriverManager.getConnection(db_url, db_user, db_password);
					logger.info(this, "Successfully connected!");
				}
			
		} catch (Exception e) {
			logger.error(this, e.toString());
			e.printStackTrace();
		}
		return conn;
	}

	 public void releaseConnection(Connection conn) {
			logger.info("releaseConnection", "Releasing HBase Connection");
			if (conn != null) {
			  try {
				conn.close();
				logger.info("releaseConnection", "Closed HBase Connection");
			  } catch (SQLException e) {
				e.printStackTrace();
				logger.error(this, e.toString());
			  }
			}
		  }
	 
	 public DBConnection getInstance(){
		 if(this.dbConn==null){
			 this.dbConn = new DBConnection();
		 }
		 return this.dbConn;
	 }

}