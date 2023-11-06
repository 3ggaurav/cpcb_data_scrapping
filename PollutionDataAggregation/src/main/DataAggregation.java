package main;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import configuration.Configurator;
import dao.AggregateDao;
import logger.AggregationLogger;

public class DataAggregation {
	
		public static enum table {
			pollution_data_temp, pollution_data_raw, location_id, parameter, value, unit, data_datetime, insert_datetime,
			location, crawled_on, id, source_id, station,
			pollution_data, PM25, PM10, SO2, NO2, CO, NH3, O3, aqi_val, aqi_contributor, status;
		}

		private static String country;
		private static Timestamp aggregationTime;
		private static int location_id;
		static String path = new File("").getAbsolutePath();

		public static void main(String[] args) throws IOException {

			AggregationLogger logger = new AggregationLogger();
			ResultSet rs;
			AggregateDao ad = new AggregateDao();
			int time_duration = Integer.parseInt(Configurator.getInstance().getProperty("aggr_dur"));
			try {
				rs = ad.selectLocationId();
				while(rs.next()) {
					location_id = rs.getInt("location_id");
					logger.info("DataAggregation", "location_id  : "+ location_id +"  picked for aggregation processing.");					
					ResultSet rs1 = ad.getCountryName(location_id);
					rs1.next();
					country = rs1.getString("country");
					
				    rs1 = ad.getParamName(country);
				    
				    aggregationTime = ad.getAggregationDateTime(time_duration);
				    
					logger.info("DataAggregation", "location_id  : "+ location_id +" All requirements are set for aggragate.");
				    ad.runAggregationFunction(rs1, location_id, time_duration, aggregationTime, country);
					logger.info("DataAggregation", "location_id  : "+ location_id +"  aggregation done.");
				    //move perticular location id data from pollution_data_temp to pollution_data_raw;
					logger.info("DataAggregation", "Now push the data from temp table to raw table for location id  :"+location_id);
				    int id1 = ad.insertDataFromTempToRaw(aggregationTime, location_id);
					logger.info("DataAggregation", "Data moved successfully for location id  :"+location_id +"in raw table.");

				    //delete perticular id from pollution_data_temp;
					logger.info("DataAggregation", "Now delete the data from temp table for location id  :"+location_id);
				    rs1 = ad.deleteAggregateDataFromTemp(aggregationTime, location_id);
					logger.info("DataAggregation", "Data deleted successfully from temp table for location id  :"+location_id);

					logger.info("DataAggregation", "location id  :"+location_id +"successfully processed.");
			}
				
				logger.info("DataAggregation", "**** AGRREGATION DONE ***** \n\n till TIME : "+aggregationTime);
			}catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				logger.error("main_crawl", "SQLException   :"+e1.toString());
			}
		}
}


//IF(ISTEXT(C8),0,IF(C8<=50,C8,IF(AND(C8>50,C8<=100),C8,IF(AND(C8>100,C8<=250),100+(C8-100)*100/150,IF(AND(C8>250,C8<=350),200+(C8-250),IF(AND(C8>350,C8<=430),300+(C8-350)*(100/80),IF(C8>430,400+(C8-430)*(100/80))))))))
//IF(ISTEXT(C10),0,IF(C10<=30,C10*50/30,IF(AND(C10>30,C10<=60),50+(C10-30)*50/30,IF(AND(C10>60,C10<=90),100+(C10-60)*100/30,IF(AND(C10>90,C10<=120),200+(C10-90)*(100/30),IF(AND(C10>120,C10<=250),300+(C10-120)*(100/130),IF(C10>250,400+(C10-250)*(100/130))))))))
//IF(ISTEXT(C12),0,IF(C12<=40,C12*50/40,IF(AND(C12>40,C12<=80),50+(C12-40)*50/40,IF(AND(C12>80,C12<=380),100+(C12-80)*100/300,IF(AND(C12>380,C12<=800),200+(C12-380)*(100/420),IF(AND(C12>800,C12<=1600),300+(C12-800)*(100/800),IF(C12>1600,400+(C12-1600)*(100/800))))))))
//IF(ISTEXT(C14),0,IF(C14<=40,C14*50/40,IF(AND(C14>40,C14<=80),50+(C14-40)*50/40,IF(AND(C14>80,C14<=180),100+(C14-80)*100/100,IF(AND(C14>180,C14<=280),200+(C14-180)*(100/100),IF(AND(C14>280,C14<=400),300+(C14-280)*(100/120),IF(C14>400,400+(C14-400)*(100/120))))))))
//IF(ISTEXT(C16),0,IF(C16<=1,C16*50/1,IF(AND(C16>1,C16<=2),50+(C16-1)*50/1,IF(AND(C16>2,C16<=10),100+(C16-2)*100/8,IF(AND(C16>10,C16<=17),200+(C16-10)*(100/7),IF(AND(C16>17,C16<=34),300+(C16-17)*(100/17),IF(C16>34,400+(C16-34)*(100/17))))))))
//IF(ISTEXT(C18),0,IF(C18<=50,C18*50/50,IF(AND(C18>50,C18<=100),50+(C18-50)*50/50,IF(AND(C18>100,C18<=168),100+(C18-100)*100/68,IF(AND(C18>168,C18<=208),200+(C18-168)*(100/40),IF(AND(C18>208,C18<=748),300+(C18-208)*(100/539),IF(C18>748,400+(C18-400)*(100/539))))))))
//IF(ISTEXT(C20),0,IF(C20<=200,C20*50/200,IF(AND(C20>200,C20<=400),50+(C20-200)*50/200,IF(AND(C20>400,C20<=800),100+(C20-400)*100/400,IF(AND(C20>800,C20<=1200),200+(C20-800)*(100/400),IF(AND(C20>1200,C20<=1800),300+(C20-1200)*(100/600),IF(C20>1800,400+(C20-1800)*(100/600))))))))
