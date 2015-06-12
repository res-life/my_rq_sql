package com.yeahmobi.datasystem.query.meta;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yangxu on 5/12/14.
 */

public final class TableDefaults {

    public static TableSpec of(Class<? extends TableSpec> tClass) {
        if (tClass == dimension.getClass()) {
            return dimension;
        } else if (tClass == metric.getClass()) {
            return metric;
        }

        throw new IllegalArgumentException("unknown TableSpec Impl:" + tClass.getCanonicalName());
    }

    public static final DimensionTable dimension = new DimensionTable();
    static {

        Map<String, DimensionDetail> table = new HashMap<String, DimensionDetail>();

        table.put("aff_id", new DimensionDetail("aff_id", "aff_id", ValueType.INTEGER, 0));
        table.put("aff_manager", new DimensionDetail("aff_manager", "aff_manager", ValueType.INTEGER, 0));
        table.put("aff_sub1", new DimensionDetail("aff_sub1", "aff_sub1", ValueType.STRING, ""));
        table.put("aff_sub2", new DimensionDetail("aff_sub2", "aff_sub2", ValueType.STRING, ""));
        table.put("aff_sub3", new DimensionDetail("aff_sub3", "aff_sub3", ValueType.STRING, ""));
        table.put("aff_sub4", new DimensionDetail("aff_sub4", "aff_sub4", ValueType.STRING, ""));
        table.put("aff_sub5", new DimensionDetail("aff_sub5", "aff_sub5", ValueType.STRING, ""));
        table.put("aff_sub6", new DimensionDetail("aff_sub6", "aff_sub6", ValueType.STRING, ""));
        table.put("aff_sub7", new DimensionDetail("aff_sub7", "aff_sub7", ValueType.STRING, ""));
        table.put("aff_sub8", new DimensionDetail("aff_sub8", "aff_sub8", ValueType.STRING, ""));
        table.put("adv_id", new DimensionDetail("adv_id", "adv_id", ValueType.INTEGER, 0));
        table.put("adv_manager", new DimensionDetail("adv_manager", "adv_manager", ValueType.INTEGER, 0));
        table.put("adv_sub1", new DimensionDetail("adv_sub1", "adv_sub1", ValueType.STRING, ""));
        table.put("adv_sub2", new DimensionDetail("adv_sub2", "adv_sub2", ValueType.STRING, ""));
        table.put("adv_sub3", new DimensionDetail("adv_sub3", "adv_sub3", ValueType.STRING, ""));
        table.put("adv_sub4", new DimensionDetail("adv_sub4", "adv_sub4", ValueType.STRING, ""));
        table.put("adv_sub5", new DimensionDetail("adv_sub5", "adv_sub5", ValueType.STRING, ""));
        table.put("adv_sub6", new DimensionDetail("adv_sub6", "adv_sub6", ValueType.STRING, ""));
        table.put("adv_sub7", new DimensionDetail("adv_sub7", "adv_sub7", ValueType.STRING, ""));
        table.put("adv_sub8", new DimensionDetail("adv_sub8", "adv_sub8", ValueType.STRING, ""));
        table.put("offer_id", new DimensionDetail("offer_id", "offer_id", ValueType.INTEGER, 0));
        table.put("currency", new DimensionDetail("currency", "currency", ValueType.STRING, ""));
        table.put("rpa", new DimensionDetail("rpa", "rpa", ValueType.DECIMAL, 0.000f));
        table.put("cpa", new DimensionDetail("cpa", "cpa", ValueType.DECIMAL, 0.000f));
        table.put("ref_track", new DimensionDetail("ref_track", "ref_track", ValueType.STRING, ""));
        table.put("ref_track_site", new DimensionDetail("ref_track_site", "ref_track_site", ValueType.STRING, ""));
        table.put("click_ip", new DimensionDetail("click_ip", "click_ip", ValueType.STRING, ""));
        table.put("conv_ip", new DimensionDetail("conv_ip", "conv_ip", ValueType.STRING, ""));
        table.put("transaction_id", new DimensionDetail("transaction_id", "transaction_id", ValueType.STRING, ""));
        table.put("click_time", new DimensionDetail("click_time", "click_time", ValueType.STRING, ""));
        table.put("conv_time", new DimensionDetail("conv_time", "conv_time", ValueType.STRING, ""));
        table.put("user_agent", new DimensionDetail("user_agent", "user_agent", ValueType.STRING, ""));
        table.put("browser", new DimensionDetail("browser", "browser", ValueType.INTEGER, 0));
        table.put("device_brand", new DimensionDetail("device_brand", "device_brand", ValueType.INTEGER, 0));
        table.put("device_model", new DimensionDetail("device_model", "device_model", ValueType.INTEGER, 0));
        table.put("device_os", new DimensionDetail("device_os", "device_os", ValueType.INTEGER, 0));
        table.put("device_type", new DimensionDetail("device_type", "device_type", ValueType.INTEGER, 0));
        table.put("country", new DimensionDetail("country", "country", ValueType.STRING, ""));
        table.put("log_tye", new DimensionDetail("log_tye", "log_tye", ValueType.INTEGER, 0));
        table.put("visitor_id", new DimensionDetail("visitor_id", "visitor_id", ValueType.STRING, ""));
        table.put("x_forwarded_for", new DimensionDetail("x_forwarded_for", "x_forwarded_for", ValueType.STRING, ""));
        table.put("state", new DimensionDetail("state", "state", ValueType.INTEGER, 0));
        table.put("city", new DimensionDetail("city", "city", ValueType.INTEGER, 0));
        table.put("isp", new DimensionDetail("isp", "isp", ValueType.INTEGER, 0));
        table.put("mobile_brand", new DimensionDetail("mobile_brand", "mobile_brand", ValueType.INTEGER, 0));
        table.put("platform_id", new DimensionDetail("platform_id", "platform_id", ValueType.INTEGER, 0));
        table.put("screen_width", new DimensionDetail("screen_width", "screen_width", ValueType.INTEGER, 0));
        table.put("screen_height", new DimensionDetail("screen_height", "screen_height", ValueType.INTEGER, 0));
        table.put("type_id", new DimensionDetail("type_id", "type_id", ValueType.INTEGER, 0));
        table.put("conversions", new DimensionDetail("conversions", "conversions", ValueType.INTEGER, 0));
        table.put("track_type", new DimensionDetail("track_type", "track_type", ValueType.INTEGER, 0));
        table.put("session_id", new DimensionDetail("session_id", "session_id", ValueType.STRING, ""));
        table.put("visitor_node_id", new DimensionDetail("visitor_node_id", "visitor_node_id", ValueType.STRING, ""));
        table.put("expiration_date", new DimensionDetail("expiration_date", "expiration_date", ValueType.STRING, ""));
        table.put("is_unique_click", new DimensionDetail("is_unique_click", "is_unique_click", ValueType.INTEGER, 0));
        table.put("gcid", new DimensionDetail("gcid", "gcid", ValueType.STRING, ""));
        table.put("gcname", new DimensionDetail("gcname", "gcname", ValueType.STRING, ""));

        dimension.setTable(table);
    }

    public final static MetricAggTable metric = new MetricAggTable();

    static {
        Map<String, MetricDetail> table = new HashMap<String, MetricDetail>();
        // metrics
        table.put("click", new MetricDetail("click", "sum(click)", "(longsum(click) as click)", 0, 1));
        table.put("conversion", new MetricDetail("conversion", "sum(conversion)", "(longsum(conversion) as conversion)", 0, 1));
        table.put("unique_click", new MetricDetail("unique_click", "sum(unique_click)", "(longsum(unique_click) as unique_click)", 0, 1));
        table.put("cr", new MetricDetail("cr", "(conversion/click)", "(longsum(conversion) / longsum(click) as cr)", 4, 2));
        table.put("cost", new MetricDetail("cost", "sum(cost)", "(doublesum(cost) as cost)", 3, 2));
        table.put("revenue", new MetricDetail("revenue", "sum(revenue)", "(doublesum(revenue) as revenue)", 3, 2));
        table.put("profit", new MetricDetail("profit", "(revenue-cost)", "((doublesum(revenue) - doublesum(cost)) as profit)", 3, 2));
        table.put("cpc", new MetricDetail("cpc", "(cost/click)", "(doublesum(cost) / longsum(click) as cpc)", 3 ,2));
        table.put("epc", new MetricDetail("epc", "(cost/click)", "(doublesum(cost) / longsum(click) as epc)", 3, 2));
        table.put("rpc", new MetricDetail("rpc", "(revenue/click)", "(doublesum(revenue) / longsum(click) as rpc)", 3, 2));
        table.put("arpa", new MetricDetail("arpa", "(revenue/conversion)", "(doublesum(revenue) / longsum(conversion) as arpa)", 2, 2));
        table.put("acpa", new MetricDetail("acpa", "(cost/conversion)", "(doublesum(cost) / longsum(conversion) as acpa)", 2, 2));

        table.put("test", new MetricDetail("test", "test", "(js(test:conversion,rpa:[a,b,\"a*b\"]:+:0))", 3, 2));
        metric.setTable(table);
    }

}
