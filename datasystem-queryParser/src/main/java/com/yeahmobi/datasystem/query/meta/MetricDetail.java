package com.yeahmobi.datasystem.query.meta;

/**
 * Created by yangxu on 3/17/14.
 */

import com.google.common.base.Objects;
import org.apache.log4j.Logger;

/**
 * 主要是将原始统计指标转换一下名称（方便后期druid处理）
 * 
 * @author yangxu
 * 
 */
public class MetricDetail {

    private static Logger logger = Logger.getLogger(MetricDetail.class);
    String alisa;// 别名
    String name;// 一般是null
    String formula;// 公式描述
    int precision;// 精度
    int level = 1;

    public MetricDetail() {

    }

    public MetricDetail(String alisa, String name,
                        String formula, int precision,
                        int level) {
        this.alisa = alisa;
        this.name = name;
        this.formula = formula;
        this.precision = precision;
        this.level = level;
    }

    public String getAlisa() {
        return alisa;
    }

    public String getName() {
        return name;
    }

    public String getFormula() {
        return formula;
    }

    public int getPrecision() {
        return precision;
    }

    public void setAlisa(String alisa) {
        this.alisa = alisa;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setFormula(String formula) {
        this.formula = formula;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("alisa", alisa)
                .add("name", name)
                .add("formula", formula)
                .add("precision", precision)
                .add("level", level)
                .toString();
    }
}
