package com.yeahmobi.datasystem.query.meta;

import com.yeahmobi.datasystem.query.antlr4.DruidReportParser;
import com.yeahmobi.datasystem.query.skeleton.PostContext;
import com.yeahmobi.datasystem.query.skeleton.PostProcess;

/**
 * 查询实体
 * 
 * @author martin
 * 
 */
public class QueryEntry {
    private PostProcess postProcess;
    private String query_str = "";
    private String url;
	private boolean isImpala;
	private String dataSource;
	private DruidReportParser parser;
	private String format;
	private PostContext postContext;
	
    public QueryEntry(PostProcess postProcess, String query_str, String url, boolean isImpala, String dataSource, DruidReportParser parser, String format, PostContext postContext) {
        super();
        this.postProcess = postProcess;
        this.query_str = query_str;
        this.url = url;
        this.isImpala = isImpala;
        this.dataSource = dataSource;
        this.parser = parser;
        this.format = format;
        this.postContext = postContext; 
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public PostProcess getPostProcess() {
        return postProcess;
    }

    public void setPostProcess(PostProcess postProcess) {
        this.postProcess = postProcess;
    }

    public String getQuery_str() {
        return query_str;
    }

    public void setQuery_str(String query_str) {
        this.query_str = query_str;
    }

	public boolean isImpala() {
		return isImpala;
	}

	public String getDataSource() {
		return dataSource;
	}

	public DruidReportParser getParser() {
		return parser;
	}

	public String getFormat() {
		return format;
	}

	public PostContext getPostContext() {
		return postContext;
	}
	
}
