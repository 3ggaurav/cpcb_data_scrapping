package utility;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import logger.AggregationLogger;

public class Utility {

	AggregationLogger logger = new AggregationLogger();

	private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static String timezone = "UTC";

	public Utility() {

	}

	public static String humanReadableByteCount(long bytes) {
		int unit = 1024;
		if (bytes < unit)
			return bytes + " B";
		int exp = (int) (Math.log(bytes) / Math.log(unit));
		char pre = ("KMGTPE").charAt(exp - 1);
		return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
	}

	public static String humanReadableTime(long timeMiliSec) {
		dateFormat.setTimeZone(TimeZone.getTimeZone(timezone));
		return dateFormat.format(new Date(timeMiliSec));

	}

	/*
	 * Returns time elapsed since input time in human readable format
	 * 
	 * @param fromTimeMiliSec
	 */
	public static String humanReadableTimeElapsed(long fromTimeMiliSec) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeZone(TimeZone.getTimeZone(timezone));
		return humanReadableTimeElapsed(fromTimeMiliSec, cal.getTimeInMillis());
	}

	/**
	 * Convert string to Calendar object
	 * 
	 * @param date
	 * @return
	 */
	public static Calendar dateToCalanderObject(String date) {
		Calendar calenderValue = Calendar.getInstance();
		calenderValue.setTimeZone(TimeZone.getTimeZone(timezone));
		try {
			Date parsedDate = dateFormat.parse(date);
			calenderValue.setTime(parsedDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return calenderValue;
	}

	public static Calendar timeStampToCalander(Timestamp time) {
		Calendar calenderValue = Calendar.getInstance();
		calenderValue.setTimeZone(TimeZone.getTimeZone(timezone));
		calenderValue.setTimeInMillis(time.getTime());
		return calenderValue;
	}

	public static Timestamp getSecondsToTimeStamp(long timeInSecondsOrMiliseconds) {
		String timeSecStr = "" + timeInSecondsOrMiliseconds;
		if (timeSecStr.length() > 10)
			timeInSecondsOrMiliseconds = timeInSecondsOrMiliseconds / 1000;
		Calendar calenderValue = Calendar.getInstance();
		calenderValue.setTimeZone(TimeZone.getTimeZone(timezone));
		calenderValue.setTimeInMillis(timeInSecondsOrMiliseconds);
		Timestamp ttime = new Timestamp(calenderValue.getTime().getTime());
		return ttime;
	}

	public static Calendar getSecondsToCalendar(long timeInSecondsOrMiliseconds) {
		String timeSecStr = "" + timeInSecondsOrMiliseconds;
		if (timeSecStr.length() > 10)
			timeInSecondsOrMiliseconds = timeInSecondsOrMiliseconds / 1000;
		Calendar calenderValue = Calendar.getInstance();
		calenderValue.setTimeZone(TimeZone.getTimeZone(timezone));
		calenderValue.setTimeInMillis(timeInSecondsOrMiliseconds);
		return calenderValue;
	}

	/*
	 * Returns time difference between input times in human readable format
	 * 
	 * @param fromTimeMiliSec
	 * 
	 * @param toTimeMiliSec
	 */
	public static String humanReadableTimeElapsed(long fromTimeMiliSec, long toTimeMiliSec) {
		long diff = toTimeMiliSec - fromTimeMiliSec;

		long diffSeconds = diff / 1000 % 60;
		long diffMinutes = diff / (60 * 1000) % 60;
		long diffHours = diff / (60 * 60 * 1000) % 24;
		long diffDays = diff / (24 * 60 * 60 * 1000);

		StringBuffer elapsedTime = new StringBuffer();

		if (diffDays > 0) {
			elapsedTime.append(diffDays + " day, ");
		}
		if (diffHours > 0) {
			elapsedTime.append(diffHours + " hr, ");
		}
		if (diffMinutes > 0) {
			elapsedTime.append(diffMinutes + " min, ");
		}
		elapsedTime.append(diffSeconds + " sec ");
		return elapsedTime.toString();

	}

	public static Calendar getCurrentTime() {
		Calendar cal = Calendar.getInstance();
		cal.setTimeZone(TimeZone.getTimeZone(timezone));
		return cal;
	}


	public static Calendar getFutureTime(Calendar calenderObj, long miliSec) {
		if (calenderObj.getTimeZone() == null) {
			calenderObj.setTimeZone(TimeZone.getTimeZone(timezone));
		}
		calenderObj.add(Calendar.SECOND, (int) miliSec);
		return calenderObj;
	}

	public static float divideScale2(float value, float divisor) {
		BigDecimal divisorBD = new BigDecimal(divisor);
		return (new BigDecimal(value)).divide(divisorBD, 2, RoundingMode.HALF_UP).floatValue();
	}

	public static float addScale2(float value1, float value2) {
		return (new BigDecimal(value1 + value2)).setScale(2, RoundingMode.HALF_UP).floatValue();
	}

	
	public static int convertStringToInteger(String inputSource) {
		if (UtilityHelper.isStringEmpty(inputSource)) {
			return 0;
		}
		try {
			return Integer.parseInt(inputSource.trim().toString());
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
	}
	 public static Calendar getDateDifferent(int timeDiff) {
			Calendar cal = Utility.getCurrentTime();
			cal.add(Calendar.MINUTE, timeDiff);
			return cal;

	}
}