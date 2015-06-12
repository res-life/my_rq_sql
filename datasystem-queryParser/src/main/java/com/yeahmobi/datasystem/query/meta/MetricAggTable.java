package com.yeahmobi.datasystem.query.meta;

/**
 * Created by yangxu on 3/17/14.
 */

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.CharMatcher;
import com.yeahmobi.datasystem.query.exception.ReportRuntimeException;
import com.yeahmobi.datasystem.query.serializer.ObjectSerializer;

/**
 * 初始化信息 针对是统计指标信息
 * 
 * @author yangxu
 * 
 */
final public class MetricAggTable implements TableSpec {

    private static Logger logger = Logger.getLogger(MetricAggTable.class);

    private Map<String, MetricDetail> table;// KEY表示前台统计的信息，value是存放转化信息

    private final Map<String, String> l1 = new HashMap<String, String>();
    private final Map<String, String> l2 = new HashMap<String, String>();

	private ClassLoader classLoader;

	private String dataSource;

    public MetricAggTable() {
    	this.classLoader = Thread.currentThread().getContextClassLoader();
    }
    
    public MetricAggTable(ClassLoader classloader) {
    	this.classLoader = classloader;
    }

    @Override
    public void init(String dataSourceName) {
    	this.dataSource = dataSourceName;
        init();
    }

    @Override
    public void copy(TableSpec spec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void init() {
        table = ObjectSerializer.read(dataSource + "_metric.json", new TypeReference<Map<String, MetricDetail>>()
        {
        }, classLoader);

        for (MetricDetail detail : table.values()) {
            if (detail.level == 1) {
                l1.put(detail.getName(), detail.getAlisa());
            } else if (detail.level == 2) {
                // l2.put(detail.getName(), detail.getAlisa());
                l2.put(detail.getAlisa(), detail.getName());
            } else {
                throw new ReportRuntimeException("metric table level should be 1 or 2, %s is %s", detail.getName(), detail.level);
            }
        }
        logger.info(ObjectSerializer.write(table).toString());
    }

    public String getAggExpr(String metric) {
        MetricDetail detail = table.get(CharMatcher.is('"').trimFrom(metric).toLowerCase());
        if (null != detail) {
            return detail.formula;
        }
        return null;
    }

    public String getL1Alias(String name) {
        return l1.get(CharMatcher.anyOf(" \"").trimFrom(name).toLowerCase());
    }

    public String getL2Name(String alias) {
        return l2.get(CharMatcher.anyOf(" \"").trimFrom(alias).toLowerCase());
    }
 
    public int getAggPrecision(String metric) {
        MetricDetail detail = table.get(CharMatcher.is('"').trimFrom(metric).toLowerCase());
        if (null != detail) {
            return detail.precision;
        }
        return 0;
    }

    public void persist() {

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        try {
            objectMapper.writeValue(new File(MetricAggTable.class.getSimpleName() + ".json"), table);
        } catch (IOException e) {
            logger.error("", e);
        }
    }

    public Map<String, MetricDetail> getTable() {
        return table;
    }

    public void setTable(Map<String, MetricDetail> table) {
        this.table = table;
    }

	@Override
	public String getDataSource() {
		return dataSource;
	}

	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}
}
