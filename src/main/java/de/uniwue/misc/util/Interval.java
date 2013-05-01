package de.uniwue.misc.util;

import java.util.Comparator;

public class Interval {

  public Integer begin, end;

  public Interval(int aBegin, int anEnd) {
    begin = aBegin;
    end = anEnd;
  }


  @Override public String toString() {
  	return begin + " -> " + end;
  }


  public boolean contains(int aNumber) {
    return (begin <= aNumber) && (end >= aNumber);
  }


  public static class IntervalComparator implements Comparator<Interval> {

    public int compare(Interval s1, Interval s2){
      int result = s1.begin.compareTo(s2.begin);
      if (result == 0) {
        return s1.begin.compareTo(s2.begin);
      } else {
        return result;
      }
    }

  }
}
