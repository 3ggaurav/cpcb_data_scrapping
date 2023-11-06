package main;

import java.util.HashMap;

public class AqiCalculationIndia {

	public static Object[] aqi_calculation(HashMap<String, Object> dataset){
		double max_aqi_val = 0.0;
		String aqi_contributor = "";
		Object[] maxAqi = new Object[2];
		
		HashMap<String, Double> param_aqi = new HashMap<String, Double>();
		if(dataset.containsKey("PM25")){
			double pm25 = (double)dataset.get("PM25");
			if(pm25 <= 30){
				pm25 = pm25*50/30;
			}
			else if(pm25 > 30 && pm25 <= 60){
				pm25 = 50+(pm25-30)*50/30;
			}
			else if(pm25 > 60 && pm25 <= 90){
				pm25 = 100 +(pm25 - 60)*100/30;
			}
			else if(pm25 > 90 && pm25 <= 120){
				pm25 = 200+(pm25 - 90)*(100/30);
			}
			else if(pm25 > 120 && pm25 <= 250){
				pm25 = 300 +(pm25 - 120)*(100/130);
			}
			else if(pm25 > 250){
				pm25 = 400+(pm25 - 250)*(100/130);
			}
			if(pm25  > max_aqi_val){
				max_aqi_val = pm25;
				aqi_contributor = "PM25";
			}
		}
		
		if(dataset.containsKey("PM10")){
			param_aqi.put("PM10", (Double)dataset.get("PM10"));
			double pm10 = param_aqi.get("PM10");
			if(pm10 > 100 && pm10 <=250){
				pm10 = 100 + (pm10 - 100)*100/150;
			}
			else if(pm10 > 250 && pm10 <=350){
				pm10 = 200 + (pm10 - 250);
			}
			else if(pm10 > 350 && pm10 <=430){
				pm10 = 300 + (pm10 - 350)*100/80;
			}
			else if(pm10 >430){
				pm10 = 400 + (pm10 - 430)*100/80;
			}
			if(pm10 > max_aqi_val){
				max_aqi_val = pm10;
				aqi_contributor = "PM10";
			}
		}
		
		if(dataset.containsKey("NO2")){
			double no2 = (double)dataset.get("NO2");
			if(no2 <= 40){
				no2 = no2 * 50/40;
			}
			else if(no2 > 40 && no2 <= 80){
				no2 = 50+(no2 - 40)*50/40;
			}
			else if(no2 > 80 && no2 <= 180){
				no2 = 100+(no2 -80)*100/100;
			}
			else if(no2 > 180 && no2 <= 280){
				no2 = 200+(no2 -180)*(100/100);
			}
			else if(no2 > 280 && no2 <=400){
				no2 = 300+(no2 - 280)*(100/120);
			}
			else if(no2 > 400){
				no2 = 400+(no2 - 400)*(100/120);
			}
			if(no2  > max_aqi_val){
				max_aqi_val = no2;
				aqi_contributor = "NO2";
			}
		}
		
		if(dataset.containsKey("SO2")){
			double so2 = (double)dataset.get("SO2");
			if(so2 <= 40) {
				so2 = so2*50/40;
			}
			else if(so2 > 40 && so2 <= 80){
				so2 = 50+(so2 - 40)*50/40;
			}
			else if(so2 > 80 && so2 <= 380){
				so2 = 100+(so2 - 80)*100/300;
			}
			else if(so2 > 380 && so2 <= 800){
				so2 = 200+(so2 - 380)*(100/420);
			}
			else if(so2 > 800 && so2 <= 1600){
				so2 = 300+(so2 - 800)*(100/800);
			}
			else if(so2 > 1600){
				so2 = 400+(so2 - 1600)*(100/800);
			}
			if(so2  > max_aqi_val){
				max_aqi_val = so2;
				aqi_contributor = "SO2";
			}
		}
		
		if(dataset.containsKey("CO")){
			double co = (double)dataset.get("CO")/1000;
			if(co <= 1){
				co = co * 50/1;
			}
			else if(co > 1 && co <= 2){
				co = 50 + (co - 1)*50/1;
			}
			else if(co > 2 && co <= 10){
				co = 100+(co - 2)*100/8;
			}
			else if(co > 10 && co <= 17){
				co = 200+(co -10)*(100/7);
			}
			else if(co > 17 && co <=34){
				co = 300+(co -17)*(100/17);
			}
			else if(co > 34){
				co = 400+(co -34)*(100/17);
			}
			if(co  > max_aqi_val){
				max_aqi_val = co;
				aqi_contributor = "CO";
			}
		}
		
		if(dataset.containsKey("NH3")){
			double nh3 = (double) dataset.get("NH3");
			if(nh3 <= 200){
				nh3 = nh3 * 50/200 ;
			}
			else if(nh3 > 200 && nh3 <= 400){
				nh3 = 50+(nh3 -200)*50/200;
			}
			else if(nh3 > 400 && nh3 <= 800){
				nh3 = 100+(nh3 -400)*100/400;
			}
			else if(nh3 > 800 && nh3 <=1200){
				nh3 =200+(nh3 -800)*(100/400);
			}
			else if(nh3 > 1200 && nh3 <=1800){
				nh3 = 300+(nh3 -1200)*(100/600);
			}
			else if(nh3 > 1800){
				nh3 = 400+(nh3 -1800)*(100/600);
			}
			if(nh3  > max_aqi_val){
				max_aqi_val = nh3;
				aqi_contributor = "NH3";
			}
		}
		
		if(dataset.containsKey("O3")){
			double o3 = (double)dataset.get("O3");
			if(o3 <=50 ){
				o3 = o3*50/50;
			}
			else if(o3 > 50 && o3 <= 100){
				o3 = 50 + (o3 - 50)*50/50;
			}
			else if(o3 > 100 && o3 <= 168){
				o3 = 100 + (o3 -100)*100/68;
			}
			else if(o3 > 168 && o3 <= 208) {
				o3 = 200 + (o3 - 168)*(100/40);
			}
			else if(o3 > 208 && o3 <= 748){
				o3 = 300+(o3 -208)*(100/539);
			}
			else if(o3 > 748){
				o3 = 400+(o3 -400)*(100/539);
			}
			if(o3  > max_aqi_val){
				max_aqi_val = o3;
				aqi_contributor = "O3";
			}
		}
		
		maxAqi[0] = aqi_contributor;
		maxAqi[1] = max_aqi_val;
		return maxAqi;
	}
}
