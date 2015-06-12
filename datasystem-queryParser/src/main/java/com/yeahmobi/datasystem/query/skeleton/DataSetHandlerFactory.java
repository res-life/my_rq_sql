package com.yeahmobi.datasystem.query.skeleton;


/**
 * common data set handler 生成器
 *
 */
public class DataSetHandlerFactory {

	/**
	 * return the common handler that all data source will use<br>
	 * the handler uses Chain of Responsibility pattern
	 * 
	 * @return
	 */
	public static DataSetHandler createHandler(PostContext request) {

		if (request.getReportContext().isDoTimeDb() || request.isTimeSort()) {

			// 时间维度
			TimeDimentionDataSetHandler timeDimentionhandler = new TimeDimentionDataSetHandler(request);
			return timeDimentionhandler;

		} else {
			DataSetHandler commonHandler = new DefaultDataSetHandler();
			return commonHandler;
		}

	}

}
