package com.yeahmobi.datasystem.query.meta;

public class DataSource {
    public class DS{
        String data_source;

        public DS() {}
        public DS(String data_source) {
            this.data_source = data_source;
        }
        public String getData_source() {
            return data_source;
        }
        public void setData_source(String data_source) {
            this.data_source = data_source;
        }
    }
    private String name;
    private String dimension_file;
    private String metric_agg_file;
    
    public DataSource() {}
    public DataSource(String data_source, String dimension_file, String metric_agg_file) {
        this.name = data_source;
        this.dimension_file = dimension_file;
        this.metric_agg_file = metric_agg_file;
    }
    
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getDimension_file() {
        return dimension_file;
    }
    public void setDimension_file(String dimension_file) {
        this.dimension_file = dimension_file;
    }
    public String getMetric_agg_file() {
        return metric_agg_file;
    }
    public void setMetric_agg_file(String metric_agg_file) {
        this.metric_agg_file = metric_agg_file;
    }
}

