package com.yeahmobi.datasystem.query.meta;

public enum TokenType {
	notEquals("=") {
		@Override
		public String convert() {
			return "!=";
		}
	},
	equals("!=") {
		@Override
		public String convert() {
			return "=";
		}
	},greaterThan(">") {
		@Override
		public String convert() {
			// TODO Auto-generated method stub
			return null;
		}
	};
	
	final private String token;
	
	private TokenType(String token) {
        this.token = token;
    }
	
	abstract public String convert();
	
	public String getToken(){
		return token;
	}
}
