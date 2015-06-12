package com.yeahmobi.datasystem.query.process;
/**
 * Created by yangxu on 3/17/14.
 */

import com.google.common.base.Function;
import com.yeahmobi.datasystem.query.meta.IntervalUnit;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;

public class TimeStampTransformer implements Function<String, Object> {

    private static Logger logger = Logger.getLogger(TimeStampTransformer.class);

    public TimeStampTransformer(long mills, DateTimeZone timeZone) {
        this.mills = mills;
        this.timeZone = timeZone;

    }
    final long mills;
    final DateTimeZone timeZone;
    @Nullable
    @Override
    public Object apply(@Nullable String input) {
        return IntervalUnit.valueOf(input.toUpperCase()).convert(mills, timeZone);
    }
    
}
