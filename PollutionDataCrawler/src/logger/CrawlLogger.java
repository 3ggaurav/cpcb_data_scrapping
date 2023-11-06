package logger;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class CrawlLogger {

		private Logger logger = null;
		public static boolean schedulerLog = true;

		  public static final boolean TRACE = true;
		  public static final boolean DEBUG = true;
		  public static final boolean INFO = true;
		  public static final boolean ERROR = true;
		  public static final boolean WARNING = true; 
		  
	//	
		  public CrawlLogger() {
			logger = LogManager.getRootLogger();
		  }

		  public CrawlLogger(String applicationName) {
			  
			logger = LogManager.getLogger(applicationName);
		  }

		  public void trace(Object c, String msg) {
			if (TRACE) {
//			  System.out.println("[" + c.getClass().getName() + "]" + msg);
			  logger.trace("[" + c.getClass().getName() + "]" + msg);
			}

		  }

		  public void debug(Object c, String msg) {
			if (DEBUG) {
			  logger.debug("[" + c.getClass().getName() + "]" + msg);		  
			}
		  }

		  public void debug(Object c, String msg, Throwable params) {
			if (DEBUG) {
			   logger.debug("[" + c.getClass().getName() + "]" + msg, params);		   
			}
		  }

		  public void error(Object c, String msg) {
			if (ERROR)
			  logger.error("[" + c.getClass().getName() + "]" + msg);
		  }

		  public void error(Object c, String msg, Throwable ex) {
			if (ERROR)
				System.out.println(msg);
			  logger.error("[" + c.getClass().getName() + "]" + msg, ex);
		  }
		  
		  public void warning(Object c, String msg) {
				if (WARNING)
				  logger.warn("[" + c.getClass().getName() + "]" + msg);
			  }

			  public void warning(Object c, String msg, Throwable params) {
				if (WARNING)
				  logger.warn("[" + c.getClass().getName() + "]" + msg, params);
			  }

		  public void info(Object c, String msg) {
			if (INFO)
				System.out.println(msg);
			  logger.info("[" + c.getClass().getName() + "]" + msg);
		  } 

	}