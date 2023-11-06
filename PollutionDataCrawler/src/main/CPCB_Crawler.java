package main;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import logger.CrawlLogger;

import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import configuration.Configurator;
import dao.CrawlDao;


public class CPCB_Crawler {
	

	public static enum table {
		pollution_data_temp, location_id, parameter, value, unit, data_datetime, insert_datetime,
		location, crawled_on, id, latitude, longitude,
		pollution_params, pollutant_name, reading_type;
	}

	private static String base_link = "https://api.openaq.org/v1/measurements?order_by=date";
	private static int fetchlimit = 10000;
	private static String localdate, country, city, station, state, crawled_on, final_link;
	private static int location_id;
	static String path = new File("").getAbsolutePath();
	static Document doc = null;

	public static void main(String[] args) throws IOException {

		//Document doc1 = Jsoup.connect(link).get();
		CrawlLogger logger = new CrawlLogger();
		ResultSet rs, rsParamUnit;
		try {
			rs = new CrawlDao().getLocationData();
			
			while(rs.next()) {
				boolean flag = true;
				final_link="";
				location_id = rs.getInt("id");
				country = rs.getString("country");
				rsParamUnit = new CrawlDao().getParamUnit(country);
				HashMap<String, String> paramHash = new HashMap<String, String>();
				while(rsParamUnit.next()){
					paramHash.put(rsParamUnit.getString(table.pollutant_name.toString()), rsParamUnit.getString(table.reading_type.toString()));
				}
				state = rs.getString("state");
				city = rs.getString("city");
				station = rs.getString("station");
				crawled_on = rs.getString("crawled_on");
				
				final_link = base_link + "&location=" + station;
				final_link += "&limit=" + fetchlimit;
				
				
				//System.exit(0);
				logger.info(new CPCB_Crawler(), "location_id:"+location_id+" country:"+country+" state:"+state+" city:"+city+" station:"+station);
				doc = null;
				if(crawled_on==null){	
					final_link += "&date_from=" + new SimpleDateFormat("yyyy-MM-dd").format(new Date(new Date().getTime()-((long)7*24*3600*1000)));
					final_link += "&sort=asc";
				}
				else{
					final_link += "&date_from=" + crawled_on;
					final_link += "&sort=asc";
				}
				logger.info("CPCB_Crawler link", final_link);
				

				try{
					doc = Jsoup.connect(final_link).ignoreContentType(true).timeout(10*1000).get();
					}
				 catch (IOException e) {
					 logger.error("jsoup error", "jsoup IOException  :"+e.toString());
					e.printStackTrace();
					continue;
				}
				
				String json_string = doc.getElementsByTag("body").first().text();
				logger.debug("json_data", json_string);
				JSONObject js = new JSONObject(json_string);
				JSONArray result = (JSONArray) js.get("results");
				for(int i=0; i< result.length();i++) {
					JSONObject dataset = result.getJSONObject(i);
					
					HashMap<String, Object> hashset = new HashMap<String, Object>();
					hashset.put(table.location_id.toString(), location_id);
					String parameter = dataset.getString("parameter").toUpperCase();
					hashset.put(table.parameter.toString(), parameter);
					if(paramHash.containsKey(parameter)){
						if(!dataset.getString("unit").equalsIgnoreCase(paramHash.get(parameter))){
							if(dataset.getString("unit").equalsIgnoreCase("µg/m³")){
								Object[] unitandvalue = ugto(dataset.getDouble("value"),paramHash.get(parameter));
								hashset.put(table.value.toString(), (double)unitandvalue[1]);
								hashset.put(table.unit.toString(), (String)unitandvalue[0]);
							
							}
							else if(dataset.getString("unit").equalsIgnoreCase("mg/m³")){
								Object[] unitandvalue = mgto(dataset.getDouble("value"),paramHash.get(parameter));
								hashset.put(table.value.toString(), (double)unitandvalue[1]);
								hashset.put(table.unit.toString(), (String)unitandvalue[0]);
							
							}
						}
						else {
							hashset.put(table.value.toString(), dataset.getDouble("value"));
							hashset.put(table.unit.toString(), dataset.getString("unit"));
						}
					}
					else{
						hashset.put(table.value.toString(), dataset.getDouble("value"));
						hashset.put(table.unit.toString(), dataset.getString("unit"));
					}
					
					localdate = ((JSONObject)dataset.get("date")).get("local").toString();
					localdate = localdate.split("\\+")[0];
					localdate = localdate.replaceAll("T", " ");
					try {
						hashset.put(table.data_datetime.toString(), new Timestamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(localdate).getTime()));
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						logger.error("parse", "ParseException   :"+e.toString());
					}
					hashset.put(table.insert_datetime.toString(), new Timestamp(new Date().getTime()));
					
					int id = new CrawlDao().insertPollutionData(hashset, table.pollution_data_temp.toString(), table.id.toString());
					logger.info("insertion", "Data inserted  : "+hashset.toString());
					
					HashMap<String, Object> hashset1 = new HashMap<String, Object>();
					hashset1.put(table.crawled_on.toString(), ((JSONObject)dataset.get("date")).get("utc").toString());
					if(Configurator.getInstance().getProperty("latlongflag").trim().equalsIgnoreCase("1") && flag) {
						hashset1.put(table.latitude.toString(), ((double)((JSONObject)dataset.get("coordinates")).get("latitude")));
						hashset1.put(table.longitude.toString(), ((double)((JSONObject)dataset.get("coordinates")).get("longitude")));
						flag = false;
					}
					hashset1.put(table.id.toString(), location_id);
					ResultSet rs1 = new CrawlDao().updateLastCrawl(hashset1, table.location.toString(), table.id.toString());
					logger.info("updation", "last crawled time updated  : "+hashset1.toString());
				}
				try {
					logger.info("waiting", "waiting for 5 sec.");
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					logger.error("main_crawl", "InterruptionException   :"+e.toString());
				}
			}
			logger.info("finish", "Crawling is done.");
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			logger.error("main_crawl", "SQLException   :"+e1.toString());
		}
	}
	
	public static Object[] mgto(double value, String funit){
		Object[] obj = new Object[2];
		if(funit.equalsIgnoreCase("µg/m³")){
			value = value * 1000;
		}
		obj[0] = funit;
		obj[1] = value;
		return obj;
	}
	public static Object[] ugto(double value, String funit){
		Object[] obj = new Object[2];
		if(funit.equalsIgnoreCase("mg/m³")){
			value = value / 1000;
		}
		obj[0] = funit;
		obj[1] = value;
		return obj;
	}
}