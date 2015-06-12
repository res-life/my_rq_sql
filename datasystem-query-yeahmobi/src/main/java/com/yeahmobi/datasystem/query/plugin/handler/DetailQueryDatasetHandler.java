package com.yeahmobi.datasystem.query.plugin.handler;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.yeahmobi.datasystem.query.config.ConfigKeyConst;
import com.yeahmobi.datasystem.query.config.ConfigLoader;
import com.yeahmobi.datasystem.query.extensions.FormatterGenerator;
import com.yeahmobi.datasystem.query.meta.ReportResult;
import com.yeahmobi.datasystem.query.plugin.meta.DetailConstant;
import com.yeahmobi.datasystem.query.skeleton.DataSet;
import com.yeahmobi.datasystem.query.skeleton.DefaultDataSetHandler;
import com.yeahmobi.datasystem.query.skeleton.Formatters;
import com.yeahmobi.datasystem.query.skeleton.PostContext;
import com.yeahmobi.datasystem.query.skeleton.ReportResultDataSet;
import com.yeahmobi.datasystem.query.skeleton.ReportServiceResultDataSet;

public class DetailQueryDatasetHandler extends DefaultDataSetHandler {
    private File file;
    private final static Logger logger = Logger.getLogger(DetailQueryDatasetHandler.class);
    Map<String, String> fileUrlMap = Maps.newHashMap();
    private PostContext reportFeatures;
    private String path="";
    private boolean isRemoveHead = false;
    private ReportResult reportResult; 
    private static String callBackUrl = "http://api.yeahmobi.com/Report/setReferraFileReady?unique_key=%s&file_url=%s&verification_code=%s";

    static {
        PropertiesConfiguration cfg = ConfigLoader.getInstance().getConfigurations();
        if (null != cfg) {
            callBackUrl = ConfigKeyConst.parseStr(cfg, callBackUrl, ConfigKeyConst.YEAHMOBI_SERVICE_CALLBACK, null, logger);
        }
    }

    public DetailQueryDatasetHandler(PostContext reportFeatures) {
        this.reportFeatures = reportFeatures;
    }

    @Override
    public DataSet processDataSet(DataSet dataSet) {
    
        inFormat(dataSet, this.reportFeatures.getReportContext().getFormat());

        DataSet newDataSet = new ReportServiceResultDataSet(dataSet.getReportServiceResult(), fileUrlMap);

        return super.processDataSet(newDataSet);
    }

    public void inFormat(DataSet dataSet, String format) {
        try {
            if (null != ((ReportResultDataSet) dataSet).getReportServiceResult()) {
                // 使用fortter插件生成格式
                FormatterGenerator formatterGenerator = Formatters.getFormatters().get(format);
                if(!Strings.isNullOrEmpty(path))
                {
                	file=new File(path);
                }
                
                if(null == file || !file.exists())
                {
                   file = File.createTempFile(reportFeatures.getReportParam().getSettings().getReport_id() + "-", "." + format);
                   this.path=file.getPath();
                }
                fileUrlMap.put(DetailConstant.FILE_URL, file.getPath());
                fileUrlMap.put(DetailConstant.CALLBACK_URL, callBackUrl);
                try(BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file, true))){
                	if(isRemoveHead){
                		reportResult = (ReportResult) dataSet.getReportResult();
                		if(reportResult.getData().getData().size() == 1)
                			return;
                		reportResult.getData().setData(reportResult.getData().getData().subList(1, reportResult.getData().getData().size()));
                	}else{
                		reportResult = (ReportResult) dataSet.getReportResult();
                		this.isRemoveHead = true;
                	}
                	formatterGenerator.write(reportResult, outputStream);
                	outputStream.flush();                	
                }
            }
        } catch (IOException e) {
            logger.error("", e);
        }
    }

}
