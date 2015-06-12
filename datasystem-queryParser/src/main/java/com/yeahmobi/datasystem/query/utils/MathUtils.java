package com.yeahmobi.datasystem.query.utils;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URLDecoder;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import com.google.common.base.Strings;

public class MathUtils {
	public static String numberFormat(Number number, int dcmlDigits) {
		NumberFormat numberFormat = NumberFormat.getInstance();
		numberFormat.setMaximumFractionDigits(dcmlDigits);
		return numberFormat.format(number);
	}
	
	public static String numberFormat2(Number number, int dcmlDigits) {
		StringBuilder stringBuilder = new StringBuilder("#0");
		if (dcmlDigits > 0) {
			stringBuilder.append(".");
			for (int i = 0; i < dcmlDigits; i++) {
				stringBuilder.append("0");
			}
		}
		DecimalFormat decimalFormat = new DecimalFormat(stringBuilder.toString());
		return decimalFormat.format(number);
	}
	
	public static BigDecimal numberFormat3(Number number, int dcmlDigits) {
	    return new BigDecimal(String.valueOf(number)).setScale(dcmlDigits, BigDecimal.ROUND_HALF_UP);
	}
	
	public static int getPrecisionDigits(String val) {
	    int prcsDigit = 0;
        if (!Strings.isNullOrEmpty(val) && val.contains(".")) {
            String tmpString = val.substring(val.indexOf(".") + 1, val.length());
            prcsDigit = tmpString.length();
        }
        return prcsDigit;
    }
	
	public static void main(String[] args) {
        String value = "0.00";
        System.out.println(MathUtils.getPrecisionDigits(value));
        
        System.out.println(MathUtils.numberFormat2(0.0000000, MathUtils.getPrecisionDigits(value)));
        System.out.println(MathUtils.numberFormat3(126.0000000, MathUtils.getPrecisionDigits(value)));
        
        String tmp = "http://172.20.0.69:8080/realquery/report?report_param={%22settings%22:{%22report_id%22:%221402919015%22,%22return_format%22:%22json%22,%22time%22:{%22start%22:1404086400,%22end%22:1404863999,%22timezone%22:0},%22data_source%22:%22ymds_druid_datasource%22,%22pagination%22:{%22size%22:10,%22page%22:0}},%22filters%22:{%22$and%22:{}},%22data%22:[%22unique_click%22,%22click%22,%22conversion%22,%22rows%22,%22cost%22,%22revenue%22,%22profit%22],%22group%22:[%22aff_id%22,%22transaction_id%22,%22itvl_hour%22,%22currency%22],%22currency_type%22:%22CNY%22}";
        try {
            System.out.println(URLDecoder.decode(tmp, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        
    }
}
