package de.uniwue.misc.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.StringEscapeUtils;

public class StringUtilsUniWue {


  public static String unescapeXml(String anXMLString) {
    return StringEscapeUtils.unescapeXml(anXMLString);
  }


  public static String concat(String[] someStrings, String separator) {
    StringBuilder builder = new StringBuilder();
    for (String aString : someStrings) {
      builder.append(aString);
      builder.append(separator);
    }
    if (builder.length() > 0) {
      builder.delete(builder.length() - separator.length(), builder.length());
    }
    return builder.toString();
  }


  public static String concatLongs(Collection<Long> someLongs, String separator) {
  	List<String> stringList = new ArrayList<String>();
  	for (Long aLong : someLongs) {
  		stringList.add(Long.toString(aLong));
  	}
  	return concat(stringList, separator);
  }


  public static String concat(Collection<String> someStrings, String separator) {
  	return concat(someStrings.toArray(new String[0]), separator);
  }


  public static String arffName(String aName) {
    String result = aName.replaceAll("ä", "ae");
    result = result.replaceAll("ö", "oe");
    result = result.replaceAll("ü", "ue");
    result = result.replaceAll("ß", "ss");
    result = result.replaceAll("[^\\w]", "_");
    // ( |-|/|:|=|%|\\[\\]|\\(|\\)|\\{|\\}|,|\\[|\\|°|'])
    result = result.replaceAll("^_*", "");
    result = result.replaceAll("_+", "_");
    result = result.replaceAll("_*$", "");
    return result;
  }



}
