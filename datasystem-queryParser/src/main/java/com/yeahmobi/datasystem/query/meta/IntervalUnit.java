package com.yeahmobi.datasystem.query.meta;

/**
 * Created by yangxu on 3/19/14.
 */

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

import com.google.common.collect.Ordering;

/**
 * 时间区间转化
 * 
 * @author yangxu
 * 
 */
public enum IntervalUnit {

    HOUR((int) TimeUnit.HOURS.toMinutes(1)) {
        //
        public Object convert(DateTime dateTime) {
            return dateTime.getHourOfDay();
        }

        @Override
        public Period granularityPeriod() {
            return Period.hours(1);// 每小时 value:PT1H
        }

		@Override
		public boolean filterTime(DateTime dateTime,TimeFilter timeFilter,DateTimeZone zone) {
			DateTime dateTimeMove = new DateTime(dateTime, zone);
			String hour = dateTimeMove.getHourOfDay() + "";
			String nowHour = timeFilter.getTimeSelected();
			switch (timeFilter.dimensionToken) {
			case equals:{
				if (!hour.equalsIgnoreCase(nowHour)) return true;
				break;
			}
			case notEquals:{
				if (hour.equalsIgnoreCase(nowHour)) return true;
				break;
			}
			default:
				break;
			}
			
			return false;
		}

    },

    DAY((int) TimeUnit.DAYS.toMinutes(1)) {
        //
        public Object convert(DateTime dateTime) {
            return dateTime.toString("yyyy-MM-dd");
        }

        @Override
        public Period granularityPeriod() {
            return Period.days(1);// 每天 value:P1D
        }

		@Override
		public boolean filterTime(DateTime dateTime,TimeFilter timeFilter,DateTimeZone zone) {
			DateTime dateTimeMove = new DateTime(dateTime, zone);
			String day = dateTimeMove.toString("yyyy-MM-dd");
			String nowDay = timeFilter.getTimeSelected();
			switch (timeFilter.dimensionToken) {
			case equals:{
				if (!day.equalsIgnoreCase(nowDay)) return true;
				break;
			}
			case notEquals:{
				if (day.equalsIgnoreCase(nowDay)) return true;
				break;
			}
			default:
				break;
			}
			return false;
		}

    },

    MONTH((int) TimeUnit.DAYS.toMinutes(30)) {
        //
        public Object convert(DateTime dateTime) {
            return dateTime.toString("MMM", Locale.ENGLISH);
        }

        @Override
        public Period granularityPeriod() {
            return Period.months(1);// 每月 value:P1M
        }

		@Override
		public boolean filterTime(DateTime dateTime,TimeFilter timeFilter,DateTimeZone zone) {
			DateTime dateTimeMove = new DateTime(dateTime, zone);
			String month = (String) (dateTimeMove.getMonthOfYear()>9?""+dateTime.getMonthOfYear():"0"+dateTime.getMonthOfYear());
			String nowMonth = timeFilter.getTimeSelected();
			switch (timeFilter.dimensionToken) {
			case equals:{
				if (!month.equalsIgnoreCase(nowMonth)) return true;
				break;
			}
			case notEquals:{
				if (month.equalsIgnoreCase(nowMonth)) return true;
				break;
			}
			default:
				break;
			}
			return false;
		}

    },

    YEAR((int) TimeUnit.DAYS.toMinutes(365)) {
        //
        public Object convert(DateTime dateTime) {
            return dateTime.getYear();
        }

        @Override
        public Period granularityPeriod() {
            return Period.years(1);// 每年 value:P1Y
        }

		@Override
		public boolean filterTime(DateTime dateTime,TimeFilter timeFilter,DateTimeZone zone) {
			DateTime dateTimeMove = new DateTime(dateTime, zone);
			String year = dateTimeMove.getYear() + "";
			String nowYear = timeFilter.getTimeSelected();
			switch (timeFilter.dimensionToken) {
			case equals:{
				if (!year.equalsIgnoreCase(nowYear)) return true;
				break;
			}
			case notEquals:{
				if (year.equalsIgnoreCase(nowYear)) return true;
				break;
			}
			default:
				break;
			}
			return false;
		}

    },

    WEEK((int) TimeUnit.DAYS.toMinutes(7)) {
        // Mar-4
        /*
         * CONCAT(DATE_FORMAT(create_time, '%b')," + " '-', " + "(
         * WEEK(create_time,5) - WEEK( DATE_SUB(create_time,INTERVAL
         * DAYOFMONTH(create_time)-1 DAY), 5) + 1))",
         */
        public Object convert(DateTime dateTime) {
            Calendar calendar = dateTime.toCalendar(null);
            return (dateTime.toString("MMM", Locale.ENGLISH) + "-" + calendar.get(Calendar.WEEK_OF_MONTH));
        }

        @Override
        public Period granularityPeriod() {
            return Period.weeks(1);// 每周 value:P1W
        }

		@Override
		public boolean filterTime(DateTime dateTime,TimeFilter timeFilter,DateTimeZone zone) {
			DateTime dateTimeMove = new DateTime(dateTime, zone);
			Calendar calendar = dateTimeMove.toCalendar(null);
			String month = (String) (dateTimeMove.getMonthOfYear()>9?""+dateTimeMove.getMonthOfYear():"0"+dateTimeMove.getMonthOfYear());
			String week = month + "-" + calendar.get(Calendar.WEEK_OF_MONTH);
			String nowWeek = timeFilter.getTimeSelected();
			switch (timeFilter.dimensionToken) {
			case equals:{
				if(!week.equalsIgnoreCase(nowWeek))return true;
				break;
			}
			case notEquals:{
				if (week.equalsIgnoreCase(nowWeek)) return true;
				break;
			}
			default:
				break;
			}
			return false;
		}

    },

    WEEKOFMONTH((int) TimeUnit.DAYS.toMinutes(7)) {
        // Mar-4
        /*
         * CONCAT(DATE_FORMAT(create_time, '%b')," + " '-', " + "(
         * WEEK(create_time,5) - WEEK( DATE_SUB(create_time,INTERVAL
         * DAYOFMONTH(create_time)-1 DAY), 5) + 1))",
         */
        public Object convert(DateTime dateTime) {
            Calendar calendar = dateTime.toCalendar(null);
            return (dateTime.toString("MMM", Locale.ENGLISH) + "-" + calendar.get(Calendar.WEEK_OF_MONTH));
        }

        @Override
        public Period granularityPeriod() {
            return Period.weeks(1);// 每周 value:P1W
        }

		@Override
		public boolean filterTime(DateTime dateTime,TimeFilter timeFilter,DateTimeZone zone) {
			// TODO Auto-generated method stub
			return false;
		}

    };

    final private int duration;

    private IntervalUnit(int duration) {
        this.duration = duration;
    }

    abstract public Object convert(DateTime dateTime);
    
    abstract public boolean filterTime(DateTime dateTime,TimeFilter timeFilter,DateTimeZone zone);

    public Object convert(DateTime dateTime, DateTimeZone timeZone) {
        return convert(dateTime.withZone(timeZone));
    }

    public Object convert(long epoch, DateTimeZone timeZone) {
        DateTime dateTime = new DateTime(epoch, timeZone);
        return convert(dateTime);
    }

    public Object convert(long epoch) {
        return convert(epoch, DateTimeZone.UTC);
    }

    public int granularityLevel() {
        return duration;
    }

    public abstract Period granularityPeriod();

    private final static Ordering<IntervalUnit> asc = new Ordering<IntervalUnit>() {
        public int compare(IntervalUnit lhs, IntervalUnit rhs) {
            if (null == lhs)
                return -1;
            if (null == rhs)
                return 1;
            return (lhs.duration - rhs.duration);
        }
    };

    private final static Ordering<String> asc2 = new Ordering<String>() {
        public int compare(String lhs, String rhs) {
            if (null == lhs)
                return -1;
            if (null == rhs)
                return 1;
            return (valueOf(lhs.toUpperCase()).duration - valueOf(rhs.toUpperCase()).duration);
        }
    };

    private static <T> Ordering getSortFn(T val) {
        if (null == val)
            return null;
        if (val instanceof IntervalUnit) {
            return asc;
        } else if (val instanceof String) {
            return asc2;
        }
        return null;
    }

    private static <T> IntervalUnit interval(T val) {
        if (null == val)
            return null;

        if (val instanceof IntervalUnit) {
            return (IntervalUnit) val;
        } else if (val instanceof String) {
            return valueOf(((String) val).toUpperCase());
        }
        return null;
    }

    public static <T> IntervalUnit max(List<T> units) {
        if (units == null || units.isEmpty()) {
            return null;
        }

        Ordering ordering = getSortFn(units.get(0));
        if (null != ordering) {
            return interval(ordering.sortedCopy(units).get(units.size() - 1));
        }

        return null;
    }

    public static <T> IntervalUnit min(List<T> units) {
        if (units == null || units.isEmpty()) {
            return null;
        }

        Ordering ordering = getSortFn(units.get(0));
        if (null != ordering) {
            return interval(ordering.sortedCopy(units).get(0));
        }

        return null;
    } 
    
    private static int YEAR_BINARY = 8;
    private static int MONTH_BINARY = 4;
    private static int DAY_AND_WEEK_BINARY = 2;
    private static int HOUR_BINARY = 1;
    /**
     * 用于判断时间维度选择是否是连续还是间隔
     * @param units
     * @return false 表示有间隔 true 表示连续
     */
    public static <T> boolean isContinuous(List<T> units) {
        if(units == null || units.isEmpty()) {
            return false;
        }
        int time_sum = 0;
        boolean isFoundWD = false;
        for(T time_unit : units) {
            if(time_unit instanceof String) {
                String unit = (String) time_unit;
                IntervalUnit stand_unit = IntervalTable.getTable().get(unit);
                if(stand_unit != null) {
                    switch (stand_unit) {
                        case YEAR:
                            time_sum = time_sum | YEAR_BINARY;
                            break;
                        case MONTH:
                            time_sum = time_sum | MONTH_BINARY;
                            break;
                        case WEEKOFMONTH:
                        case DAY:
                            if(!isFoundWD) {
                                time_sum = time_sum | DAY_AND_WEEK_BINARY;
                                isFoundWD = true; //当碰到周和天维度，只做一次或运算
                            }
                            break;
                        case HOUR:
                            time_sum = time_sum | HOUR_BINARY;
                            break;
                        default:
                            break;
                    }
                }
            }
        }
        // 这里用穷举的方式进行（后期会对这块算法进行改进）
        if(time_sum == 9 || time_sum == 5 || time_sum == 10 || time_sum == 11 || time_sum == 13)
            return false;
        return true;
    }
    
    /**
     * 生成复合的Period值
     * @param units
     * @return
     */
    public static <T> Period compoundIntervalUnit(List<T> units) {
        if(units == null || units.isEmpty()) {
            return null;
        }
        boolean isYear = false; //标识是否有YEAR维度
        boolean isMonth = false;//标识是否有MONTH维度
        boolean isWeek = false;//标识是否有WEEK维度
        boolean isDay = false;//标识是否有DAY维度
        boolean isHour = false;//标识是否有HOUR维度
        for(T time_unit : units) {
            if(time_unit instanceof String) {
                String unit = (String) time_unit;
                IntervalUnit stand_unit = IntervalTable.getTable().get(unit);
                if(stand_unit != null) {
                    switch (stand_unit) {
                        case YEAR:
                            isYear = true;
                            break;
                        case MONTH:
                            isMonth = true;
                            break;
                        case WEEKOFMONTH:
                            isWeek = true;
                            break;
                        case DAY:
                            isDay = true;
                            break;
                        case HOUR:
                            isHour = true;
                            break;
                        default:
                            break;
                    }
                }
            }
        }
        if(isYear && isWeek && isDay && isHour) return Period.hours(1);
        if(isYear && isWeek && isHour) return Period.weeks(1).withHours(1);
        if(isYear && isDay && isHour) return Period.hours(1);
        
        if(isYear && isMonth && isHour) return Period.years(1).withHours(1);
        if(isYear && isWeek && isDay) return Period.days(1);
        if(isYear && isHour) return Period.years(1).withHours(1);
        
        if(isMonth && isHour) return Period.years(1980).withMonths(1).withHours(1);
        if(isYear && isWeek) return Period.weeks(1);
        if(isYear && isDay) return Period.days(1);
        
        return null;
    }
    
    /**
     * 判断是否是单选
     * @param units
     * @return
     */
    public static <T> boolean isSingle(List<T> units) {
        if(units == null || units.isEmpty()) {
            throw new NullPointerException("isSingle units is null");
        }
        return units.size() == 1;
    }
    
    /**
     * 单选查询条件变化(设置1972是与其他条件进行区分)生成Period
     * 
     * @param units
     * @return
     */
    public static <T> Period singleIntervalUnit(List<T> units){
        if(units == null || units.isEmpty()) {
            return null;
        }
        Period singlePeriod = null;
        for(T time_unit : units) {
            if(time_unit instanceof String) {
                String unit = (String) time_unit;
                IntervalUnit stand_unit = IntervalTable.getTable().get(unit);
                if(stand_unit != null) {
                    switch (stand_unit) {
                        case YEAR:
                            singlePeriod = Period.years(1);
                            break;
                        case MONTH:
                            singlePeriod = Period.years(1972).withMonths(1);
                            break;
                        case WEEKOFMONTH:
                            singlePeriod = Period.years(1972).withWeeks(1);
                            break;
                        case DAY:
                            singlePeriod = Period.days(1);
                            break;
                        case HOUR:
                            singlePeriod = Period.years(1972).withHours(1);
                            break;
                        default:
                            break;
                    }
                }
            }
        }
        return singlePeriod;
    }
    
    
    /**
     * 生成连续的Period(用1976来进行区分)
     * @param units
     * @return
     */
    public static <T> Period continuousIntervalUnit(List<T> units) {
        if(units == null || units.isEmpty()) {
            return null;
        }
        boolean isYear = false; //标识是否有YEAR维度
        boolean isMonth = false;//标识是否有MONTH维度
        boolean isWeek = false;//标识是否有WEEK维度
        boolean isDay = false;//标识是否有DAY维度
        boolean isHour = false;//标识是否有HOUR维度
        for(T time_unit : units) {
            if(time_unit instanceof String) {
                String unit = (String) time_unit;
                IntervalUnit stand_unit = IntervalTable.getTable().get(unit);
                if(stand_unit != null) {
                    switch (stand_unit) {
                        case YEAR:
                            isYear = true;
                            break;
                        case MONTH:
                            isMonth = true;
                            break;
                        case WEEKOFMONTH:
                            isWeek = true;
                            break;
                        case DAY:
                            isDay = true;
                            break;
                        case HOUR:
                            isHour = true;
                            break;
                        default:
                            break;
                    }
                }
            }
        }
        if(isYear && isMonth && isWeek && isDay && isHour) return Period.hours(1);
        
        if(isYear && isMonth && isWeek && isDay) return Period.days(1);
        if(isMonth && isWeek && isDay && isHour) return Period.hours(1);
        if(isYear && isMonth && isWeek && isHour) return Period.weeks(1).withHours(1);
        if(isYear && isMonth && isDay && isHour) return Period.hours(1);
        
        if(isYear && isMonth && isWeek) return Period.weeks(1);
        if(isMonth && isWeek && isDay) return Period.days(1);
        if(isWeek && isDay && isHour) return Period.hours(1);
        if(isMonth && isWeek && isHour) return Period.years(1976).withWeeks(1).withHours(1);
        if(isMonth && isDay && isHour) return Period.hours(1);
        
        if(isYear && isMonth) return Period.months(1);
        if(isMonth && isWeek) return Period.years(1976).withWeeks(1);
        if(isMonth && isDay) return Period.days(1);
        if(isWeek && isDay) return Period.days(1);
        if(isDay && isHour) return Period.hours(1);
        if(isWeek && isHour) return Period.years(1976).withWeeks(1).withHours(1);
        
        return null;
    }
    
    public static Boolean isSingle(Period period){
    	String periodStr = period.toString();
    	if(periodStr.matches("^P(T)?1[YMWDH]$")){
    		return true;
    	}
    	return false;
    }
    
    public static void main(String[] args) throws ParseException {
    	
//    	List<String> intervalUnits = Arrays.asList("hour");
//    	Period singlePeriod = IntervalUnit.singleIntervalUnit(intervalUnits);
//    	System.out.println(Period.hours(1).toString());
//    	String aa = "P1M";
//    	System.out.println(aa.matches("^P(T)?1[YMWDH]$"));
    	
    	/*int tzMinOffset = (int)(NumberFormat.getInstance().parse("5").floatValue() * 60);
    	DateTime dateTimeMove = new DateTime(DateTime.now().withZone(DateTimeZone.UTC), DateTimeZone.forOffsetMillis((int)TimeUnit.MINUTES.toMillis(tzMinOffset)));
    	
    	System.out.println(DateTime.now().withZone(DateTimeZone.UTC));
    	System.out.println(dateTimeMove);*/
    	
    	String aa = "nuh\\\\bug";
    	System.out.println(aa.matches(".*(\\\\)+.*"));
	}

}
