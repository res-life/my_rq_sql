package com.yeahmobi.datasystem.query.meta;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/** 
* IntervalUnit Tester. 
* 
* @author <YangXu> 
* @since <pre>Mar 19, 2014</pre> 
* @version 1.0 
*/ 
public class IntervalUnitTest { 

@Before
public void before() throws Exception { 
} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: convert(DateTime dateTime) 
* 
*/ 
@Test
public void testConvertDateTime() throws Exception {

    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    DateTime dateTime = DateTime.parse("2014-03-19 06:45:31", formatter.withZoneUTC());

    Assert.assertEquals("DateTime -> ", "2014-03-19 06:45:31+0000", dateTime.toString("yyyy-MM-dd HH:mm:ssZ"));

    Assert.assertEquals(IntervalUnit.DAY + " -> ", "2014-03-19", IntervalUnit.DAY.convert(dateTime));
    Assert.assertEquals(IntervalUnit.MONTH + " -> ", "Mar", IntervalUnit.MONTH.convert(dateTime));
    Assert.assertEquals(IntervalUnit.YEAR + " -> ", 2014, IntervalUnit.YEAR.convert(dateTime));
    Assert.assertEquals(IntervalUnit.HOUR + " -> ", 6, IntervalUnit.HOUR.convert(dateTime));
    Assert.assertEquals(IntervalUnit.WEEKOFMONTH + " -> ", "Mar-4", IntervalUnit.WEEKOFMONTH.convert(dateTime));

    String[] timeStr = {
            "2013-02-09 00:01:00",
            "2014-02-07 00:01:00",
            "2014-02-22 00:01:00",
            "2013-03-16 00:01:00",
            "2013-09-19 00:01:00",
    };

    String[] weekOfMonth = {
            "Feb-2",
            "Feb-2",
            "Feb-4",
            "Mar-3",
            "Sep-3",
    };

    for (int i = 0; i < timeStr.length; ++i) {
        dateTime = DateTime.parse(timeStr[i], formatter.withZoneUTC());
        Assert.assertEquals(IntervalUnit.WEEKOFMONTH + " -> ", weekOfMonth[i], IntervalUnit.WEEKOFMONTH.convert(dateTime));
    }


}

/** 
* 
* Method: convert(long epoch, DateTimeZone timeZone) 
* 
*/ 
@Test
public void testConvertForEpochTimeZone() throws Exception { 
//TODO: Test goes here...

    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    DateTime dateTime = DateTime.parse("2014-03-19 06:45:31", formatter.withZoneUTC());

    Assert.assertEquals("DateTime -> ", "2014-03-19 06:45:31+0000", dateTime.toString("yyyy-MM-dd HH:mm:ssZ"));

    Assert.assertEquals(IntervalUnit.DAY + " -> ", "2014-03-19", IntervalUnit.DAY.convert(dateTime.getMillis(), DateTimeZone.UTC));
    Assert.assertEquals(IntervalUnit.MONTH + " -> ", "Mar", IntervalUnit.MONTH.convert(dateTime.getMillis(), DateTimeZone.UTC));
    Assert.assertEquals(IntervalUnit.YEAR + " -> ", 2014, IntervalUnit.YEAR.convert(dateTime.getMillis(), DateTimeZone.UTC));
    Assert.assertEquals(IntervalUnit.HOUR + " -> ", 6, IntervalUnit.HOUR.convert(dateTime.getMillis(), DateTimeZone.UTC));
    Assert.assertEquals(IntervalUnit.WEEKOFMONTH + " -> ", "Mar-4", IntervalUnit.WEEKOFMONTH.convert(dateTime.getMillis(), DateTimeZone.UTC));

} 

/** 
* 
* Method: convert(long epoch) 
* 
*/ 
@Test
public void testConvertEpoch() throws Exception { 
//TODO: Test goes here...
     DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    DateTime dateTime = DateTime.parse("2014-03-19 06:45:31", formatter.withZoneUTC());

    Assert.assertEquals("DateTime -> ", "2014-03-19 06:45:31+0000", dateTime.toString("yyyy-MM-dd HH:mm:ssZ"));

    Assert.assertEquals(IntervalUnit.DAY + " -> ", "2014-03-19", IntervalUnit.DAY.convert(dateTime.getMillis()));
    Assert.assertEquals(IntervalUnit.MONTH + " -> ", "Mar", IntervalUnit.MONTH.convert(dateTime.getMillis()));
    Assert.assertEquals(IntervalUnit.YEAR + " -> ", 2014, IntervalUnit.YEAR.convert(dateTime.getMillis()));
    Assert.assertEquals(IntervalUnit.HOUR + " -> ", 6, IntervalUnit.HOUR.convert(dateTime.getMillis()));
    Assert.assertEquals(IntervalUnit.WEEKOFMONTH + " -> ", "Mar-4", IntervalUnit.WEEKOFMONTH.convert(dateTime.getMillis()));


} 

    @Test
    public void testTimeDiff() {
        long secs = 1000L;

        System.out.println(TimeUnit.SECONDS.toHours(secs));
        System.out.println(TimeUnit.SECONDS.toMinutes(secs));

    }
    
    /**
     * test for time filter 
     */
    @Test
    public void testTimeFilter(){
    	DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        DateTime dateTime = DateTime.parse("2014-03-19 06:45:31", formatter.withZoneUTC());

        Assert.assertEquals(IntervalUnit.HOUR + " filter -> ", false , IntervalUnit.HOUR.filterTime(dateTime, new TimeFilter(IntervalUnit.HOUR, TokenType.equals, "6"),DateTimeZone.UTC));
        Assert.assertEquals(IntervalUnit.DAY + " filter -> ", false , IntervalUnit.DAY.filterTime(dateTime, new TimeFilter(IntervalUnit.DAY, TokenType.equals, "2014-03-19"),DateTimeZone.UTC));
        Assert.assertEquals(IntervalUnit.WEEK + " filter -> ", false , IntervalUnit.WEEK.filterTime(dateTime, new TimeFilter(IntervalUnit.WEEK, TokenType.equals, "03-4"),DateTimeZone.UTC));
        Assert.assertEquals(IntervalUnit.MONTH + " filter -> ", false , IntervalUnit.MONTH.filterTime(dateTime, new TimeFilter(IntervalUnit.MONTH, TokenType.equals, "03"),DateTimeZone.UTC));
        Assert.assertEquals(IntervalUnit.YEAR + " filter -> ", false , IntervalUnit.YEAR.filterTime(dateTime, new TimeFilter(IntervalUnit.YEAR, TokenType.equals, "2014"),DateTimeZone.UTC));
        
        Assert.assertEquals(IntervalUnit.HOUR + " filter -> ", true , IntervalUnit.HOUR.filterTime(dateTime, new TimeFilter(IntervalUnit.HOUR, TokenType.equals, "3"),DateTimeZone.UTC));
        Assert.assertEquals(IntervalUnit.DAY + " filter -> ", true , IntervalUnit.DAY.filterTime(dateTime, new TimeFilter(IntervalUnit.DAY, TokenType.equals, "2014-03-16"),DateTimeZone.UTC));
        Assert.assertEquals(IntervalUnit.WEEK + " filter -> ", true , IntervalUnit.WEEK.filterTime(dateTime, new TimeFilter(IntervalUnit.WEEK, TokenType.equals, "03-3"),DateTimeZone.UTC));
        Assert.assertEquals(IntervalUnit.MONTH + " filter -> ", true , IntervalUnit.MONTH.filterTime(dateTime, new TimeFilter(IntervalUnit.MONTH, TokenType.equals, "04"),DateTimeZone.UTC));
        Assert.assertEquals(IntervalUnit.YEAR + " filter -> ", true , IntervalUnit.YEAR.filterTime(dateTime, new TimeFilter(IntervalUnit.YEAR, TokenType.equals, "2013"),DateTimeZone.UTC));
        
        
        Assert.assertEquals("两个时间维度是否连续 is" , true , IntervalUnit.isContinuous(Arrays.asList("year","month")));
        Assert.assertEquals("两个时间维度是否连续 is" , false , IntervalUnit.isContinuous(Arrays.asList("year","week")));
        Assert.assertEquals("两个时间维度是否连续 is" , true , IntervalUnit.isContinuous(Arrays.asList("month","day")));
        Assert.assertEquals("两个时间维度是否连续 is" , false , IntervalUnit.isContinuous(Arrays.asList("year","hour")));
        Assert.assertEquals("两个时间维度是否连续 is" , false , IntervalUnit.isContinuous(Arrays.asList("year","month","hour")));
        
        Assert.assertEquals("时间转成is" , Period.hours(1) ,IntervalUnit.continuousIntervalUnit(Arrays.asList("year","month","week","day","hour")));
        
        Assert.assertEquals("判断是否是需要特殊处理" , Period.hours(1) ,IntervalUnit.isSingle(Period.hours(1)));
    }

} 
