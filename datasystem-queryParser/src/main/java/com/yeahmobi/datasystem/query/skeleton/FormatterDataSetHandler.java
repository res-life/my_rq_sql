package com.yeahmobi.datasystem.query.skeleton;

import org.apache.log4j.Logger;

import com.yeahmobi.datasystem.query.akka.QueryActor;
import com.yeahmobi.datasystem.query.extensions.FormatterGenerator;
import com.yeahmobi.datasystem.query.jersey.ReportServiceResult;
import com.yeahmobi.datasystem.query.meta.MsgType;
import com.yeahmobi.datasystem.query.meta.ReportResult;

/**
 * 
 * 用于处理返回格式的data set handler<br>
 * 调用返回格式插件， 生成文件或者字符串表示<br>
 * 如果是同步调用， 生成字符串表示； 如果是异步生成file提供下载地址<br>
 * TODO: 生成文件的逻辑, 保存csv 和 xls到文件中
 */
public class FormatterDataSetHandler extends DefaultDataSetHandler {

    private String formatter;

    private boolean isSync;

    private String reportId;

    public FormatterDataSetHandler(boolean isSync, String formatter, String reportId) {

        this.isSync = isSync;
        this.formatter = formatter;
        this.reportId = reportId;
    }

    @Override
    public DataSet processDataSet(DataSet dataSet) {

        // 这里应该能够获取ReportResult
        ReportResult result = dataSet.getReportResult();

        // 使用fortter插件生成格式
        FormatterGenerator formatterGenerator = Formatters.getFormatters().get(formatter);

        ReportServiceResult reportServiceResult = null;

        // 设置最终返回给ReportService的结果
        if (!isSync) {

            // // 异步处理
            // File file = null;
            // try {
            // file = File.createTempFile(reportId + "-", "." + formatter);
            // BufferedOutputStream outputStream = new BufferedOutputStream(new
            // FileOutputStream(file));
            // formatterGenerator.write(result, outputStream);
            //
            // // TODO refactor
            // URL url = AWSFileStore.uploadFile();
            //
            // } catch (IOException e) {
            // // TODO Auto-generated catch block
            // e.printStackTrace();
            // } finally {
            // if (file != null) {
            // if (!file.delete()) {
            // String errorMsg =
            // String.format("delete temporary file %s failed",
            // file.getAbsolutePath());
            // logger.error(errorMsg);
            // }
            // }
            // }
            // return null;
            return dataSet;
        } else {
            // 同步处理
            String str = formatterGenerator.asString(result);
            reportServiceResult = new ReportServiceResult(MsgType.success, str);
        }

        // 创建新的data set
        ReportServiceResultDataSet newDataSet = new ReportServiceResultDataSet(reportServiceResult, result);
        newDataSet.setInfo(dataSet.getInfo());
        // 如果有后续， 继续处理
        return super.processDataSet(newDataSet);
    }

    private static Logger logger = Logger.getLogger(QueryActor.class);
}
