package com.yeahmobi.datasystem.query.meta;
/**
 * 过滤时间类，用来记录查询语句对时间有哪些要求
 * @author ellis
 * 2014.10.14
 */
public class TimeFilter {
	public IntervalUnit dimension;
	public TokenType dimensionToken;
    public String timeSelected;
    
	public TimeFilter(IntervalUnit dimension, TokenType dimensionToken, String timeSelected) {
		super();
		this.dimension = dimension;
		this.dimensionToken = dimensionToken;
		this.timeSelected = timeSelected;
	}

	public IntervalUnit getDimension() {
		return dimension;
	}
	
	public void setDimension(IntervalUnit dimension) {
		this.dimension = dimension;
	}
	
	public TokenType getDimensionToken() {
		return dimensionToken;
	}
	
	public void setDimensionToken(TokenType dimensionToken) {
		this.dimensionToken = dimensionToken;
	}
	
	public String getTimeSelected() {
		return timeSelected;
	}
	
	public void setTimeSelected(String timeSelected) {
		this.timeSelected = timeSelected;
	}
	
}
