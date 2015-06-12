package com.yeahmobi.datasystem.query.reportrequest;

/**
 * 确定数据的范围 offset 参数会使page失效
 * 
 * @author oscar.gao
 * 
 */
public class DataRange {

	public static int getStart(int page, int size, int offset) {
		if (-1 != offset) {
			return offset;
		} else {
			return size * page;
		}
	}

	public static int getEnd(int page, int size, int offset) {
		if (-1 != offset) {
			return offset + size;
		} else {
			if (size > 0) {
				return (page + 1) * size;
			} else if (size == 0) {
				// size 为0， 表示请求的size为无穷大
				return Integer.MAX_VALUE;
			} else {
				// size为负数， 设置默认page size为1000
				return (page + 1) * 1000;
			}
		}
	}
	
	public static boolean isFromBegin(int page, int offset){
		if(-1 == offset){
			return 0 == page;
		}else if (0 == offset){
			return true;
		}else{
			return false;
		}
	}
}
