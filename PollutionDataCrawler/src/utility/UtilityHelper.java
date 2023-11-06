package utility;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import logger.CrawlLogger;
import dao.BaseDao.fieldType;

public class UtilityHelper {
	CrawlLogger logger = new CrawlLogger();

  public static boolean isStringEmpty(String input) {
	boolean empty = true;
	if (input != null && !input.trim().equalsIgnoreCase("")) {
	  empty = false;
	}
	return empty;
  }

  public static String getFileExtenstionFromFileName(String fileName) {
	String extension = "";
	int lastIndexOfDotOperent = fileName.lastIndexOf('.');
	if (lastIndexOfDotOperent > 0) {
	  extension = fileName.substring(lastIndexOfDotOperent + 1);
	}
	return extension;
  }

  public static String cleanPath(String xPath) {
	xPath = xPath.replaceAll("\\\\", "/");
	// xPath = xPath.replaceAll("\/\/","/");
	return xPath;
  }

  public void tryToConvertIntoInteger(Object inputValue) {
	try {
	  int inputValueToInt = Integer.parseInt(inputValue.toString());
	  if ((int) inputValueToInt <= 0) {
		// String[] exceptionParams = new String[] { inputValue.toString() };
	  }
	} catch (Exception e) {
		e.printStackTrace();
		logger.error(this, e.toString());
	  // String[] exceptionParams = new String[] { inputValue.toString() };
	}
  }

  /**
   * This Method prepare Pagination object
   * 
   * @param queryParmeter
   *          : Map<String, String> queryString Key,value
   * @return Pagination Object
   */
  
  public void createXmlFileFromObject(Object obj, Class<?> classObj, String location) {
	try {
	  JAXBContext context = JAXBContext.newInstance(classObj);
	  Marshaller m = context.createMarshaller();
	  // for pretty-print XML in JAXB
	  m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
	  // Write to File
	  m.marshal(obj, new File(location));
	} catch (JAXBException e) {
		e.printStackTrace();
		logger.error(this, e.toString());
	}
  }

  public static void copyInputStream(InputStream in, OutputStream out) throws IOException {
	byte[] buffer = new byte[2048];
	int len;
	while ((len = in.read(buffer)) >= 0)
	  out.write(buffer, 0, len);
	in.close();
	out.close();
  }

  /**
   * This method converts the password string to MD5
   * 
   * @throws NoSuchAlgorithmException
   */

  public static ArrayList<HashMap<fieldType, Object>> prepareDaoDefaultArgumentsObject() {
	ArrayList<HashMap<fieldType, Object>> queryRawData = new ArrayList<HashMap<fieldType, Object>>();
	return queryRawData;
  }

  public static ArrayList<HashMap<fieldType, Object>> addValueInDaoArguments(fieldType fieldType, Object fieldValue, ArrayList<HashMap<fieldType, Object>> queryRawData) {
	ArrayList<HashMap<fieldType, Object>> arguments = queryRawData;
	HashMap<fieldType, Object> queryParmeters = new HashMap<fieldType, Object>();
	if (fieldValue != null) {
	  queryParmeters.put(fieldType, fieldValue);
	  arguments.add(queryParmeters);
	} else {
	  queryParmeters.put(fieldType, "");
	  arguments.add(queryParmeters);
	}
	return arguments;
  }

  /**
   * This method gives you the base path of the resource folder
   * 
   * @return resource folder base path
   */

  public static String getHttpAssetPath(String assetPath) {
	String path = "";
	if (!UtilityHelper.isStringEmpty(assetPath)) {

	  try {
		path = "http://" + InetAddress.getLocalHost().getHostAddress() + ":8050/asset" + assetPath;
	  } catch (UnknownHostException e) {
		  e.printStackTrace();
	  }
	}
	return path;
  }

  public static Iterable<MatchResult> allMatches(final Pattern p, final CharSequence input) {
	return new Iterable<MatchResult>() {

	  public Iterator<MatchResult> iterator() {
		return new Iterator<MatchResult>() {

		  // Use a matcher internally.
		  final Matcher matcher = p.matcher(input);
		  // Keep a match around that supports any interleaving of hasNext/next
		  // calls.
		  MatchResult pending;

		  public boolean hasNext() {
			// Lazily fill pending, and avoid calling find() multiple times if
			// the
			// clients call hasNext() repeatedly before sampling via next().
			if (pending == null && matcher.find()) {
			  pending = matcher.toMatchResult();
			}
			return pending != null;
		  }

		  public MatchResult next() {
			// Fill pending if necessary (as when clients call next() without
			// checking hasNext()), throw if not possible.
			if (!hasNext()) {
			  return null;
			}
			// Consume pending so next call to hasNext() does a find().
			MatchResult next = pending;
			pending = null;
			return next;
		  }

		  /** Required to satisfy the interface, but unsupported. */
		  public void remove() {
		  }
		};
	  }
	};
  }

  // get error bundle methods

  public static String ensureJsonFormatAsInput(String inputJson) {
	if (UtilityHelper.isStringEmpty(inputJson)) {
	  return "";
	}
	String returnStrring = inputJson.toString().replaceAll("\\|\\|\\|quot\\|\\|\\|", "\"");
	return returnStrring;
  }

}