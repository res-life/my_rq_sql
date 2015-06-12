package com.yeahmobi.datasystem.query.meta;
/**
 * Created by yangxu on 3/17/14.
 */

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.CharMatcher;
import com.yeahmobi.datasystem.query.serializer.ObjectSerializer;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * 维度表信息 默认的datasource是ymds_druid_datasource
 * @author chenyi
 *
 */
final public class DimensionTable implements TableSpec {

    private static Logger logger = Logger.getLogger(DimensionTable.class);

    private Map<String, DimensionDetail> table;
    private String dataSource = "ymds_druid_datasource";

	private ClassLoader classLoader;

    public DimensionTable(ClassLoader classloader) {
    	this.classLoader = classloader;
    }
    
    public DimensionTable() {
    	this.classLoader = Thread.currentThread().getContextClassLoader();
    }

    @Override
    public void init(String dataSource) {
        this.dataSource = dataSource;
        init();
    }

    @Override
    public void init() {
        table = ObjectSerializer.read(dataSource + "_dimention.json", new TypeReference<Map<String,DimensionDetail>>() { }, classLoader);
    }

    @Override
    public void copy(TableSpec spec) {
        if (spec instanceof DimensionTable) {
            this.dataSource = ((DimensionTable)spec).dataSource;
            this.table = ((DimensionTable)spec).table;
        }
        throw new IllegalArgumentException("mismatch class type " + spec.getClass()
                + ", expected" + this.getClass());
    }

    public ValueType getValueType(String dim) {
        DimensionDetail detail = table.get(CharMatcher.is('"').trimFrom(dim).toLowerCase());
        if (null != detail) {
            return detail.getValueType();
        }
        return ValueType.UNKNOWN;
    }

    public Object getDefaultValue(String dim) {
        DimensionDetail detail = table.get(CharMatcher.is('"').trimFrom(dim).toLowerCase());
        if (null != detail) {
            return detail.getDefaultValue();
        }
        return null;
    }

    public Map<String, DimensionDetail> getTable() {
        return table;
    }

    public void setTable(Map<String, DimensionDetail> table) {
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
