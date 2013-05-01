package de.uniwue.misc.util;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Calendar;


public class EnvironmentUniWue {

  public static String sdf_withTimeString_withoutDots = "yyyyMMdd HHmmss";
  public static String sdf_withTimeString = "dd.MM.yyyy HH:mm:ss";
  public static String sdf_withoutTimeString = "dd.MM.yyyy";
  public static SimpleDateFormat sdf_withTime_withoutDots = new SimpleDateFormat(sdf_withTimeString_withoutDots);
  public static SimpleDateFormat sdf_withTime = new SimpleDateFormat(sdf_withTimeString);
  public static SimpleDateFormat sdf_withoutTime = new SimpleDateFormat(sdf_withoutTimeString);
  public static File mainDataDir = new File(".");
  public static File codeDir = new File(".");
  public static Calendar cal = Calendar.getInstance();


  public EnvironmentUniWue() {
  }


  public static String getTimeString(long aTime) {
  	cal.setTimeInMillis(aTime);
		String timeString = EnvironmentUniWue.sdf_withTime.format(cal.getTime());
		return timeString;
  }


  public static boolean isMSSQL() {
		boolean msSQL;

		String computerName = null;
		try {
			computerName = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
		}
		if ((computerName != null) && computerName.equals("wklw161v")) {
			msSQL = true;
		} else {
			msSQL = false;
		}
		return msSQL;
  }


}
