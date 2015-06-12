package com.yeahmobi.datasystem.query.akka.cache;

import com.yeahmobi.datasystem.query.antlr4.DruidReportParser;
import com.yeahmobi.datasystem.query.reportrequest.ReportParam;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;

import java.text.NumberFormat;
import java.util.concurrent.TimeUnit;

/**
 * Created by yangxu on 5/9/14.
 */
public enum Ttls implements TTLFunc {

    Forever {
        @Override
        public long apply(Object... inputs) {
            return -1l;
        }
    },

    Dynamic {
        @Override
        public long apply(Object... inputs) {
            if (null != inputs && inputs.length != 0) {
                ReportParam request = (ReportParam)inputs[0];
                DateTime start = new DateTime(1000L * request.getSettings().getTime().getStart(), DateTimeZone.UTC);
                DateTime end = new DateTime(1000L * request.getSettings().getTime().getEnd(), DateTimeZone.UTC);
                Interval interval = new org.joda.time.Interval(start, end);
                
                // startDate is 1 hour and 10 mins before
                if (interval.isBefore(DateTime.now().minus(Duration.standardMinutes(66)))) {
                    return TimeUnit.HOURS.toSeconds(1);
                }
            }
            return TimeUnit.SECONDS.toSeconds(10);
        }
    },

    Constant { // 10s
        @Override
        public long apply(Object... inputs) {
            return TimeUnit.SECONDS.toMillis(10);
        }
    },
    
    OneMinute{
        @Override
        public long apply(Object... objs) {
            return TimeUnit.MINUTES.toSeconds(1);
        }
    },
    OneQuater{
        @Override
        public long apply(Object... objs) {
            return TimeUnit.MINUTES.toSeconds(15);
        }
    },
    HalfAnHour{
        @Override
        public long apply(Object... objs) {
            return TimeUnit.MINUTES.toSeconds(30);
        }
    },
    OneHour{
        @Override
        public long apply(Object... objs) {
            return TimeUnit.HOURS.toSeconds(1);
        }
    },
    OneDay{
        @Override
        public long apply(Object... objs) {
            return TimeUnit.DAYS.toSeconds(1);
        }
    },
    HalfADay{
        @Override
        public long apply(Object... objs) {
            return TimeUnit.HOURS.toSeconds(12);
        }
    },

}
