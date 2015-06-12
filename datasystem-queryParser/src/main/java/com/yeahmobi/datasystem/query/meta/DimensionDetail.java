package com.yeahmobi.datasystem.query.meta;

/**
 * Created by yangxu on 3/18/14.
 */


/**
 * 维度明细信息
 * 
 * @author chenyi
 * 
 */
public class DimensionDetail {

//    private static Logger logger = Logger.getLogger(DimensionDetail.class);

    private String name;//名称
    private String alisa;//别名
    private ValueType valueType;//类型
    private Object defaultValue;//默认值
    
    // 最大的长度， 如果需要二次处理，存数据库时， 创表需要. 只对STRING类型有用, 默认值是100
    private int maxLength = 100;  
    
	public DimensionDetail() {
    }

    public DimensionDetail(String name, String alisa, ValueType valueType, Object defaultValue) {
        this.name = name;
        this.alisa = alisa;
        this.valueType = valueType;
        this.defaultValue = defaultValue;
    }

    public ValueType getValueType() {
        return valueType;
    }

    public void setValueType(ValueType valueType) {
        this.valueType = valueType;
    }

    public String getAlisa() {
        return alisa;
    }

    public void setAlisa(String alisa) {
        this.alisa = alisa;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
    }

    public int getMaxLength() {
		return maxLength;
	}

	public void setMaxLength(int maxLength) {
		this.maxLength = maxLength;
	}
}
