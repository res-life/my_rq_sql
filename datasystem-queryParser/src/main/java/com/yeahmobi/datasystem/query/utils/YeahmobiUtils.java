package com.yeahmobi.datasystem.query.utils;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.yeahmobi.datasystem.query.meta.MsgType;
import com.yeahmobi.datasystem.query.meta.ReportResult;
import com.yeahmobi.datasystem.query.meta.DataSource.DS;
import com.yeahmobi.datasystem.query.meta.XCHANGE_RATE;
import com.yeahmobi.datasystem.query.meta.XCHANGE_RATE_BASE;

public class YeahmobiUtils {
    public static String getDataSourceFromQuery(String query) {
        Gson gson = new Gson();
        String[] params = query.split(",");
        for (String string : params) {
            if (string.startsWith("\"") && string.contains("data_source")) {
                DS dataSource = gson.fromJson("{" + string + "}", DS.class);
                return dataSource.getData_source();
            }
        }
        return "";
    }
    public static List<XCHANGE_RATE_BASE> ListXRatesToListXRatesBase(List<XCHANGE_RATE> xchange_rates) {
        List<XCHANGE_RATE_BASE> xchange_rate_bases = null;
        if (null != xchange_rates || xchange_rates.size() > 0) {
            xchange_rate_bases = new ArrayList<XCHANGE_RATE_BASE>();
            for (XCHANGE_RATE xchange_rate : xchange_rates) {
                XCHANGE_RATE_BASE xchange_rate_base = new XCHANGE_RATE_BASE(xchange_rate.getCurrency_from(), xchange_rate.getCurrency_to(),
                        xchange_rate.getRate_from_to(), xchange_rate.getRate_usd_to());
                xchange_rate_bases.add(xchange_rate_base);
            }
        }
        return xchange_rate_bases;
    }
    /**
     * 方法名     : rmBlankSpace
     * 描述: <p>去除参数字符串中的所有空格</p>
     * @modify by  : 
     * @modify date:
     * @param sourceStr
     * @return
     * ADD BY zhouxy AT TIME 2013-1-6 下午2:55:42
     */
    public static String rmBlankSpace(String sourceStr) {
        //...
        sourceStr = sourceStr.replace(" ", "");
        return sourceStr;
    }
    
    /**
     * 方法名     : rmWhiteSpace
     * 描述: <p>去除参数字符串当中的所有空白字符，包括回车、换行符、制表符 和 所有空格</p>
     * @modify by  : 
     * @modify date:
     * @param sourceStr
     * @return
     * ADD BY zhouxy AT TIME 2013-1-6 下午2:58:18
     */
    public static String rmWhiteSpace(String sourceStr) {
        //...
        sourceStr = sourceStr.replaceAll("\\s", "");
        return sourceStr;
    }
    
    /**
     * <p>MD5加密算法</p>
     * @since V1.0
     * @Author Martin
     * @createTime 2014年5月19日 上午10:55:04
     * @modifiedBy name
     * @modifyOn dateTime
     * @param arg0
     * @return
     */
    public final static String MD5(String arg0) {
        //用于加密的字符
        char md5String[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
                'A', 'B', 'C', 'D', 'E', 'F' };
        try {
            //使用平台的默认字符集将此 String 编码为 byte序列，并将结果存储到一个新的 byte数组中
            byte[] btInput = arg0.getBytes();
            
            // 获得指定摘要算法的 MessageDigest对象，此处为MD5
            //MessageDigest类为应用程序提供信息摘要算法的功能，如 MD5 或 SHA 算法。
            //信息摘要是安全的单向哈希函数，它接收任意大小的数据，并输出固定长度的哈希值。 
            MessageDigest mdInst = MessageDigest.getInstance("MD5");
            //System.out.println(mdInst);  
            //MD5 Message Digest from SUN, <initialized>
            
            //MessageDigest对象通过使用 update方法处理数据， 使用指定的byte数组更新摘要
            mdInst.update(btInput);
            //System.out.println(mdInst);  
            //MD5 Message Digest from SUN, <in progress>
            
            // 摘要更新之后，通过调用digest（）执行哈希计算，获得密文
            byte[] md = mdInst.digest();
            //System.out.println(md);
            
            // 把密文转换成十六进制的字符串形式
            int j = md.length;
            //System.out.println(j);
            char str[] = new char[j * 2];
            int k = 0;
            for (int i = 0; i < j; i++) {   //  i = 0
                byte byte0 = md[i];  //95
                str[k++] = md5String[byte0 >>> 4 & 0xf];    //    5  
                str[k++] = md5String[byte0 & 0xf];   //   F
            }
            
            //返回经过加密后的字符串
            return new String(str);
            
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    
    public static String getReturnInfo(MsgType type, String msg) {
        String msgType = String.valueOf(type);
        if (!Strings.isNullOrEmpty(msgType) && !Strings.isNullOrEmpty(msg)) {
            ReportResult result = new ReportResult();
            result.setFlag(msgType);
            result.setMsg(msg);
            result.setData(null);
            return (new Gson().toJson(result));
        }
        return "";
    }
    
    public static void main(String[] args) {
        String callBackUrl = "http://api.yeahmobi.com/Report/setReferraFileReady?unique_key=%s&file_url=%s&verification_code=%s";
        // md5（unique_key+file_url+“Yeahmobif3899843bc09ff972ab6252ab3c3cac6”）
        String verificationCode = MD5("" + "" + "Yeahmobif3899843bc09ff972ab6252ab3c3cac6");
        System.out.println(String.format(callBackUrl, "111", "dfdf", verificationCode));
    }
}
