package configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author Alethe
 *
 */
public class Configurator {
	private static Configurator instance = null;
	private Properties defaultProps = null;
	private Properties configProps = null;
	private File configFile = new File("crawl_pollution.conf");
	//default values
	private static final String LOG_LOCATION = "logs/";
	private static final String LOG_TYPES_ENABLED = "all";
	private static final String DRIVER_TYPE = "jdbc";
	private static final String	DB_ADDRESS = "192.168.1.160:3306";
	private static final String	DB_NAME = "capperdb";
	private static final String DB_TYPE = "mysql";
	private static final String	DB_USER = "root";
	private static final String	DB_PASSWORD = "Alethe@123";
	private static final String HTTP_REST_SERVER_PORT = "8080";
	private static final String REST_API_PORT = "8182";
	private static final String AUTO_RECONNECT = "true";
	private static final String USE_SSL = "false";
	private static final String SCHEDULER_DELAY = "3";  //secs
	private static final String latlongflag = "1";
	
	public Configurator() {
		this.defaultProps = new Properties();			
		this.defaultProps.setProperty("log_location", Configurator.LOG_LOCATION);
		this.defaultProps.setProperty("log_types_enabled", Configurator.LOG_TYPES_ENABLED);
		
		//mysql connection param
		this.defaultProps.setProperty("driver_type", Configurator.DRIVER_TYPE);
		this.defaultProps.setProperty("db_type", Configurator.DB_TYPE);
		this.defaultProps.setProperty("db_address", Configurator.DB_ADDRESS);
		this.defaultProps.setProperty("db_name", Configurator.DB_NAME);
		this.defaultProps.setProperty("db_user", Configurator.DB_USER);
		this.defaultProps.setProperty("db_password", Configurator.DB_PASSWORD);
		this.defaultProps.setProperty("http_rest_server_port", Configurator.HTTP_REST_SERVER_PORT);
		this.defaultProps.setProperty("rest_api_port", Configurator.REST_API_PORT);
		this.defaultProps.setProperty("auto_reconnect", Configurator.AUTO_RECONNECT);
		this.defaultProps.setProperty("use_ssl", Configurator.USE_SSL);
		this.defaultProps.setProperty("SCHEDULER_DELAY", Configurator.SCHEDULER_DELAY);
		this.defaultProps.setProperty("latlongflag", Configurator.latlongflag);
	}
	
	public Properties getAllProperties() {
		if(this.configProps == null) {
			this.configProps = new Properties(this.defaultProps);			
		}
		
		return this.configProps;	
	}
	
	public String getProperty(String propName) {
		String propVal = "";
		if(propName != null) {			
			propName = propName.trim().toLowerCase();
		}
		if(propName.isEmpty() || propName==null) {
			return propVal;
		}
		if(this.configProps == null) {
			this.configProps = new Properties(this.defaultProps);			
		}
		try {
			propVal = this.configProps.getProperty(propName);		
		}catch(Exception e) {
			propVal = "";
		}		
		return propVal;	
	}
	
	
	public static Configurator getInstance() {
		if(Configurator.instance == null) {			
			Configurator.instance = new Configurator();
		}		
		return Configurator.instance;		
	}
	
	public void prepare() {		
		this.configProps = new Properties(this.defaultProps);		
		
		try {
			InputStream inputStream = new FileInputStream(this.configFile);
			this.configProps.load(inputStream);
			inputStream.close();
		} catch (IOException e) {
			System.out.println(this.configFile.getAbsolutePath());
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}