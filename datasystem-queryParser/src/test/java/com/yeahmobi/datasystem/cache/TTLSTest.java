package com.yeahmobi.datasystem.cache;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.yeahmobi.datasystem.query.akka.cache.Ttls;

public class TTLSTest {
    @Test
    public void getTimePeriod() {
        System.out.println(TimeUnit.DAYS.toHours(1));
        System.out.println(TimeUnit.SECONDS.toSeconds(10));
        System.out.println(TimeUnit.HOURS.toSeconds(1));
        
        System.out.println(Ttls.OneHour.apply());
    }
}
