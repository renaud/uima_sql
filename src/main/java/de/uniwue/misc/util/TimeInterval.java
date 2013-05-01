package de.uniwue.misc.util;

import java.util.Calendar;
import java.util.Date;


public class TimeInterval {

  private static Calendar calendar = Calendar.getInstance();

  public long minRange = 0;
  public String minDate;
  public long maxRange = 0;
  public String maxDate;


  public TimeInterval() {
    addMin(Calendar.YEAR, -20);
    addMax(Calendar.YEAR, 120);
  }


  public TimeInterval(long min, long max) {
  	calendar.setTimeInMillis(min);
  	calendar.set(Calendar.SECOND, 0);
    minRange = calendar.getTimeInMillis();
    minDate = EnvironmentUniWue.sdf_withTime.format(calendar.getTime());
  	calendar.setTimeInMillis(max);
  	calendar.set(Calendar.SECOND, 0);
    maxRange = calendar.getTimeInMillis();
    maxDate = EnvironmentUniWue.sdf_withTime.format(calendar.getTime());
  }


  public TimeInterval(TimeInterval aContext, long aDocTime) {
    this(aContext);
  	calendar.setTimeInMillis(aDocTime);
  	calendar.set(Calendar.SECOND, 0);
    long aRoundedDateTime = calendar.getTimeInMillis();
    minRange += aRoundedDateTime;
    minDate = EnvironmentUniWue.sdf_withTime.format(new Date(minRange));
    maxRange += aRoundedDateTime;
    maxDate = EnvironmentUniWue.sdf_withTime.format(new Date(maxRange));
  }


  public TimeInterval(TimeInterval aContext) {
    minRange = aContext.minRange;
    minDate = aContext.minDate;
    maxRange = aContext.maxRange;
    maxDate = aContext.maxDate;
  }


  public void addMin(int timeScaleType, int distance) {
  	calendar.setTime(new Date(minRange));
  	calendar.add(timeScaleType, distance);
    minRange = calendar.getTimeInMillis();
    minDate = EnvironmentUniWue.sdf_withTime.format(new Date(minRange));
  }


  public void addMax(int timeScaleType, int distance) {
  	calendar.setTime(new Date(maxRange));
  	calendar.add(timeScaleType, distance);
    maxRange = calendar.getTimeInMillis();
    maxDate = EnvironmentUniWue.sdf_withTime.format(new Date(maxRange));
  }


  public void addMin(long milliseconds) {
    minRange += milliseconds;
    minDate = EnvironmentUniWue.sdf_withTime.format(new Date(minRange));
  }


  public void addMax(long milliseconds) {
    maxRange += milliseconds;
    maxDate = EnvironmentUniWue.sdf_withTime.format(new Date(maxRange));
  }


  public void setMin(long aDist) {
    minRange = aDist;
    minDate = EnvironmentUniWue.sdf_withTime.format(new Date(minRange));
  }


  public void setMax(long aDist) {
    maxRange = aDist;
    maxDate = EnvironmentUniWue.sdf_withTime.format(new Date(maxRange));
  }


  public TimeInterval cut(TimeInterval anotherInt) {
  	return cut(anotherInt.minRange, anotherInt.maxRange);
  }


  public TimeInterval cut(long anotherMinRange, long anotherMaxRange) {
    long min = Math.max(anotherMinRange, minRange);
    long max = Math.min(anotherMaxRange, maxRange);
    TimeInterval result = null;
    if (min <= max) {
    	result = new TimeInterval(min, max);
    }
    return result;
  }

}
