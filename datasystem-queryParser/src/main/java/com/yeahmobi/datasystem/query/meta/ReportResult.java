package com.yeahmobi.datasystem.query.meta;

/**
 * Created by yangxu on 3/17/14.
 */

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.yeahmobi.datasystem.query.aws.AWSFileStore;

/**
 * 返回结果集对象
 * 
 * @author yangxu
 * 
 */
public class ReportResult {

    private static Logger logger = Logger.getLogger(ReportResult.class);
    private String flag;
    private String msg;
    private Entity data = new Entity();

    public ReportResult() {
    }

    public static class Entity {
        private ReportPage page;
        private List<Object[]> data = new ArrayList<Object[]>();

        public ReportPage getPage() {
            return page;
        }

        public void setPage(ReportPage page) {
            this.page = page;
        }

        public List<Object[]> getData() {
            return data;
        }

        public void setData(List<Object[]> data) {
            this.data = data;
        }

        String prettyPrint() {
            StringBuilder builder = new StringBuilder();
            builder.append(new Gson().toJson(page)).append("<br />");
            Joiner joiner = Joiner.on('\t').useForNull("");
            joiner.join(Lists.newArrayList(page));
            for (Object[] objects : data) {
                builder.append(joiner.join(objects)).append("<br />");
            }

            return builder.toString();
        }

        String exportExcel() {
            String uploadError = "{\"flag\":\"fail\",\"msg\":\"upload s3 error\"}";
            String zippedFilename = "";
            UUID uuid = UUID.randomUUID();

            HSSFWorkbook wb = new HSSFWorkbook();
            HSSFSheet sheet = wb.createSheet("QueryResult");
            int rowNum = 0;

            for (Object[] objects : data) {
                int colNum = 0;
                HSSFRow row = sheet.createRow(rowNum);
                rowNum++;
                for (Object obj : objects) {
                    HSSFCell cell = row.createCell(colNum);
                    cell.setCellValue(obj.toString());
                    colNum++;
                }

            }

            try {
                URL url = AWSFileStore.uploadDataAsXls(uuid.toString(), wb);
                zippedFilename = url == null ? uploadError : url.toString();

            } catch (FileNotFoundException e) {
                zippedFilename = uploadError;
                logger.error("", e);
            } catch (IOException e) {
                zippedFilename = uploadError;
                logger.error("", e);

            }

            return zippedFilename;
        }

        String asHtmlTable() {
            StringBuilder builder = new StringBuilder();
            builder.append("<table border=1 width=100%><tr>");
            builder.append(new Gson().toJson(page)).append("</tr>");
            Joiner colJoiner = Joiner.on("</td><td>").useForNull("");
            builder.append("<tr><td>");
            for (Object[] objects : data) {
                builder.append(colJoiner.join(objects));
                builder.append("</td></tr><tr><td>");
            }
            builder.append("</tr></table>");

            return builder.toString();
        }
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public Entity getData() {
        return data;
    }

    public void setData(Entity data) {
        this.data = data;
    }

    public void append(Object[] row) {
        data.data.add(row);
    }

    public void setPage(ReportPage page) {
        data.setPage(page);
    }

    public String prettyPrint() {
        return data.prettyPrint();
    }

    public String asHtmlTable() {
        return data.asHtmlTable();
    }

    public String asExport() {
        return data.exportExcel();
    }

}
