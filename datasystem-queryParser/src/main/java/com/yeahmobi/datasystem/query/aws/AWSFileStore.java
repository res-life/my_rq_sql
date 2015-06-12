package com.yeahmobi.datasystem.query.aws;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.compress.compressors.gzip.GzipUtils;
import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yeahmobi.datasystem.query.akka.QueryConfig;
import com.yeahmobi.datasystem.query.akka.QueryConfigModule;

/**
 * <p>
 * AWS文件上传
 * </p>
 * 
 * @since V1.0
 * @Author Martin
 * @createTime 2014年5月30日 下午1:05:55
 * @modifiedBy name
 * @modifyOn dateTime
 */
public class AWSFileStore {
    private static Logger logger = Logger.getLogger(AWSFileStore.class);

    private static final QueryConfig cfg;
    public static PropertiesCredentials propertiesCredentials = null;
    public static AmazonS3 conn = null;
    static {
        Injector injector = Guice.createInjector(new QueryConfigModule());
        cfg = injector.getInstance(QueryConfig.class);

        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is = classloader.getResourceAsStream("AwsCredentials.properties");
        logger.info("Loaded AWSCredentials configuration.");
        try {
            propertiesCredentials = new PropertiesCredentials(is);
            conn = new AmazonS3Client(propertiesCredentials);
        } catch (IOException e) {
            logger.error("", e);
        }
    }

    public static String Save(String data) throws IOException {
        File file = File.createTempFile("tmp-", ".csv");
        file.deleteOnExit();

        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        writer.write(data);
        writer.flush();
        writer.close();

        String zippedFilename = gzipFile(file.getAbsolutePath());
        File zippedFile = new File(zippedFilename);
        zippedFile.deleteOnExit();

        String bucketName = cfg.getBucket();
        String fileName = cfg.getDir() + zippedFile.getName();

        conn.putObject(new PutObjectRequest(bucketName, fileName, zippedFile));

        return cfg.getPrefix() + bucketName + "/" + fileName;
    }

    public static String Save(String prefix, String data) throws IOException {
        File file = File.createTempFile(prefix + "-", ".csv");
        file.deleteOnExit();

        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        writer.write(data);
        writer.flush();
        writer.close();

        String zippedFilename = gzipFile(file.getAbsolutePath());
        File zippedFile = new File(zippedFilename);
        zippedFile.deleteOnExit();

        String bucketName = cfg.getBucket();
        String fileName = cfg.getDir() + zippedFile.getName();

        conn.putObject(new PutObjectRequest(bucketName, fileName, zippedFile));

        // return "s3://" + bucketName + "/" + dir;
        return cfg.getPrefix() + bucketName + "/" + fileName;
    }

    public static URL uploadDataAsCSV(String prefix, String data) throws IOException {
        File file = File.createTempFile(prefix + "-", ".csv");
        file.deleteOnExit();

        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        writer.write(data);
        writer.flush();
        writer.close();

        String zippedFilename = gzipFile(file.getAbsolutePath());
        File zippedFile = new File(zippedFilename);
        zippedFile.deleteOnExit();

        String bucketName = cfg.getBucket();
        String fileName = cfg.getDir() + zippedFile.getName();

        conn.putObject(new PutObjectRequest(bucketName, fileName, zippedFile));

        // return
        // http://druid.yeahmobi.com.s3.amazonaws.com/query.result/report_id-m201405301630-5921815181162778289.csv.gz?Expires=1401450294&AWSAccessKeyId=AKIAIIRBULVIK2HAJF6A&Signature=NwQjoZl0hJl041fFoJdgD831iDU%3D
        // return AwsTools.generateHttpDownloadUrl(conn, bucketName, fileName);
        Date expiresDate = null;
        DateFormat dateFormat = new SimpleDateFormat(cfg.getExpir_dt_format());
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        try {
            expiresDate = dateFormat.parse(cfg.getExpires());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return AwsTools.generateHttpDownloadUrl(conn, bucketName, fileName, expiresDate);
    }

    public static URL uploadDataAsCSV(File file) throws IOException {

        String zippedFilename = gzipFile(file.getAbsolutePath());
        File zippedFile = new File(zippedFilename);
        zippedFile.deleteOnExit();

        String bucketName = cfg.getBucket();
        String fileName = cfg.getDir() + zippedFile.getName();

        conn.putObject(new PutObjectRequest(bucketName, fileName, zippedFile));

        // return
        // http://druid.yeahmobi.com.s3.amazonaws.com/query.result/report_id-m201405301630-5921815181162778289.csv.gz?Expires=1401450294&AWSAccessKeyId=AKIAIIRBULVIK2HAJF6A&Signature=NwQjoZl0hJl041fFoJdgD831iDU%3D
        // return AwsTools.generateHttpDownloadUrl(conn, bucketName, fileName);
        Date expiresDate = null;
        DateFormat dateFormat = new SimpleDateFormat(cfg.getExpir_dt_format());
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        try {
            expiresDate = dateFormat.parse(cfg.getExpires());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return AwsTools.generateHttpDownloadUrl(conn, bucketName, fileName, expiresDate);
    }

    private static String createFileToZip(File sourceFile, File zipFile) throws IOException {
        byte[] buf = new byte[1024];

        File objFile = zipFile;

        ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(objFile));

        ZipEntry ze = null;

        ze = new ZipEntry(sourceFile.getName());
        ze.setSize(sourceFile.length());
        ze.setTime(sourceFile.lastModified());

        zos.putNextEntry(ze);

        InputStream is = new BufferedInputStream(new FileInputStream(sourceFile));

        int readLen = -1;
        while ((readLen = is.read(buf, 0, 1024)) != -1) {
            zos.write(buf, 0, readLen);
        }
        is.close();
        zos.close();

        return objFile.getAbsolutePath();
    }

    public static URL uploadDataAsXls(String prefix, HSSFWorkbook wb) throws IOException {
        File file = File.createTempFile(prefix + "-", ".xls");
        file.deleteOnExit();

        File zipFile = File.createTempFile(prefix + "-", ".zip");
        zipFile.deleteOnExit();

        wb.write(new FileOutputStream(file));
        String zippedFilename = createFileToZip(file, zipFile);
        File zippedFile = new File(zippedFilename);
        zippedFile.deleteOnExit();

        String bucketName = cfg.getBucket();
        String fileName = cfg.getDir() + zippedFile.getName();

        conn.putObject(new PutObjectRequest(bucketName, fileName, zippedFile));

        Date expiresDate = null;
        DateFormat dateFormat = new SimpleDateFormat(cfg.getExpir_dt_format());
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        try {
            expiresDate = dateFormat.parse(cfg.getExpires());
        } catch (ParseException e) {
            logger.error("", e);
        }
        return AwsTools.generateHttpDownloadUrl(conn, bucketName, fileName, expiresDate);
    }

    private static String gzipFile(String filename) throws IOException {
        try {
            // Create the GZIP output stream
            String compressedFilename = GzipUtils.getCompressedFilename(filename);
            GZIPOutputStream out = new GZIPOutputStream(new FileOutputStream(compressedFilename));

            // Open the input file
            FileInputStream in = new FileInputStream(filename);

            // Transfer bytes from the input file to the GZIP output stream
            byte[] buf = new byte[1024];
            int len;
            while ((len = in.read(buf)) > 0) {
                out.write(buf, 0, len);
            }
            in.close();

            // Complete the GZIP file
            out.finish();
            out.close();

            return compressedFilename;
        } catch (IOException e) {
            throw e;
        }
    }

    public static URL uploadFile() {
        return null;
    }
}

class AwsTools {
    public static URL generateHttpDownloadUrl(AmazonS3 conn, String bucketName, String fileName) {
        GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucketName, fileName);
        return conn.generatePresignedUrl(request);
    }

    public static URL generateHttpDownloadUrl(AmazonS3 conn, String bucketName, String fileName, Date expires) {
        GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucketName, fileName);
        if (expires != null) {
            request.setExpiration(expires);
        }
        return conn.generatePresignedUrl(request);
    }
}
