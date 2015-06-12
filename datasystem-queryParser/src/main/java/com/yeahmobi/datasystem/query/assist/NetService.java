package com.yeahmobi.datasystem.query.assist;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.protocol.HTTP;
import org.apache.log4j.Logger;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yeahmobi.datasystem.query.akka.QueryConfig;
import com.yeahmobi.datasystem.query.akka.QueryConfigModule;
import com.yeahmobi.datasystem.query.antlr4.DruidReportParser;
import com.yeahmobi.datasystem.query.config.ConfigManager;
import com.yeahmobi.datasystem.query.exception.NullBRPException;
import com.yeahmobi.datasystem.query.exception.ReportParserException;
import com.yeahmobi.datasystem.query.meta.ReportContext;
import com.yeahmobi.datasystem.query.meta.XCHANGE_RATE_BASE;
import com.yeahmobi.datasystem.query.meta.XchangeQueryParam;
import com.yeahmobi.datasystem.query.meta.XchangeQueryResult;
import com.yeahmobi.datasystem.query.process.QueryContext;
import com.yeahmobi.datasystem.query.utils.YeahmobiUtils;

/**
 * <p>
 * 网络操作辅助类：获取broker地址，发送druid request请求，执行接口回调
 * </p>
 * 
 * @since V1.0
 * @Author Martin
 * @createTime 2014年5月30日 下午12:58:54
 * @modifiedBy name
 * @modifyOn dateTime
 */
public class NetService {
	private static Logger logger = Logger.getLogger(NetService.class);

	private static int connTimeout = 5000; // 5s
	private static int socketTimeout = 10000; // 10s
	private static final QueryConfig cfg;

	static {
		Injector injector = Guice.createInjector(new QueryConfigModule());
		cfg = injector.getInstance(QueryConfig.class);
	}

	private ReportContext reportContext = null;

	private NetService() {
		super();
	}

	public static NetService getNetService() {
		return new NetService();
	}

	// 获取到连接

	private static URL getBrokerServiceBRPAddr(QueryContext context) throws NullBRPException, MalformedURLException {
		// BrokerRP brp =
		// process.getFreeBrokerRP(context.getQuery().getDataSource().getNames().get(0));
		//
		// if (brp == null) {
		// throw new NullBRPException("brp is null");
		// }
		// process.addReq(brp.getKey());

		return new URL("TODO");
	}

	private static URL getBrokerAddr(ParseService parseService) throws MalformedURLException {

		String url = "http://172.0.0.47:8080/druid/v2";
		DruidReportParser parser = parseService.getDruidReportParser();
		if (Strings.isNullOrEmpty(url)) {

			url = ConfigManager.getInstance().getCfg().getDruid().getBaseUrl();

		}
		return new URL(url);
	}

	private static URL getBrokerServiceAddr() throws MalformedURLException {
		return new URL(ConfigManager.getInstance().getCfg().druid.url());
	}

	private void createReportContext(QueryContext context, DruidReportParser parser) {
		reportContext = new ReportContext(context, parser);
	}

	public InputStream fireQuery(String queryData) throws ReportParserException, IOException {
		InputStream inputStream = null;
		URL url = null;
		URLConnection urlConnection = null;
		ParseService service = ParseService.getParseService();
		// String queryString =
		// service.getJsonQueryStr(ProcessService.preProcess(queryData));
		String queryString = service.getJsonQueryStr(queryData);
		createReportContext(service.getQueryContext(), service.getDruidReportParser());
		try {
			// url = getBrokerServiceAddr();
			url = getBrokerAddr(service);
			urlConnection = url.openConnection();
			urlConnection.addRequestProperty("content-type", "application/json");
			urlConnection.setDoOutput(true);
			if (logger.isDebugEnabled()) {
				logger.debug("Calling Druid Broker Node @ URL : [" + url + "].");
			}
			urlConnection.getOutputStream().write(queryString.getBytes(Charsets.UTF_8));
			// BufferedReader stdInput = new BufferedReader(new
			// InputStreamReader(urlConnection.getInputStream()));
			inputStream = urlConnection.getInputStream();
		} catch (MalformedURLException e) {
			logger.error("", e);
		}
		return inputStream;
	}

	public static List<XCHANGE_RATE_BASE> getXchangeRateList() throws ReportParserException, IOException {
		InputStream inputStream = null;
		BufferedReader bReader = null;
		URL url = null;
		URLConnection urlConnection = null;
		List<XCHANGE_RATE_BASE> yahoo_xchange_rate_bases = null;
		XchangeQueryParam param = new XchangeQueryParam();
		param.setQuery_id(UUID.randomUUID().toString());
		param.setQuery_type(cfg.getXchangeSyncQueryType());
		param.setReturn_format(cfg.getXchangeSyncReturnFormat());
		param.setColums(cfg.getXchangeSyncColums().split(","));

		String jsonParams = new Gson().toJson(param);
		try {
			url = new URL(String.format(cfg.getXchangeSyncAPI(), URLEncoder.encode(jsonParams, "UTF-8")));
			urlConnection = url.openConnection();
			urlConnection.addRequestProperty("content-type", "application/json");
			urlConnection.setDoOutput(true);
			if (logger.isDebugEnabled()) {
				logger.debug("Calling XchangeSyncAPI @ URL : [" + url + "].");
			}
			urlConnection.getOutputStream().write(URLEncoder.encode(jsonParams, "UTF-8").getBytes(Charsets.UTF_8));
			inputStream = urlConnection.getInputStream();
			if (null != inputStream) {
				StringBuilder strBuilder = new StringBuilder();
				bReader = new BufferedReader(new InputStreamReader(inputStream));
				String line = "";
				while (null != (line = bReader.readLine())) {
					strBuilder.append(line);
				}
				XchangeQueryResult xchangeQueryResult = new Gson().fromJson(strBuilder.toString(), XchangeQueryResult.class);
				inputStream.close();
				yahoo_xchange_rate_bases = YeahmobiUtils.ListXRatesToListXRatesBase(xchangeQueryResult.getRates());
			}
		} catch (MalformedURLException e) {
			logger.error("", e);
		}
		return yahoo_xchange_rate_bases;
	}

	public static String yeahmobiCallBack3(String url) {
		// 从接口获取OfferList数据信息-JSON数组格式数据
		HttpParams httpParams = new BasicHttpParams();
		// httpParams.setParameter("params", params);
		HttpConnectionParams.setConnectionTimeout(httpParams, connTimeout);
		HttpConnectionParams.setSoTimeout(httpParams, socketTimeout);
		DefaultHttpClient client = new DefaultHttpClient(httpParams);
		BufferedReader bReader = null;
		InputStream inStream = null;

		HttpGet httpGet = new HttpGet(url);
		HttpResponse response;
		try {
			response = client.execute(httpGet);
			inStream = response.getEntity().getContent();
			bReader = new BufferedReader(new InputStreamReader(inStream));
			StringBuilder strBuilder = new StringBuilder();
			String line;
			while (null != (line = bReader.readLine())) {
				strBuilder.append(line);
			}
			String offerList = strBuilder.toString();
			logger.info("OfferList : " + offerList);
			return offerList;
		} catch (ClientProtocolException e) {
			logger.error("error:", e);
		} catch (IOException e) {
			logger.error("error:", e);
		}

		return "";
	}

	public ReportContext getReportContext() {
		return reportContext;
	}

	public static String yeahmobiCallBack(String url) {
		HttpParams httpParams = new BasicHttpParams();
		HttpConnectionParams.setConnectionTimeout(httpParams, (int) TimeUnit.SECONDS.toMillis(cfg.getConnTimeout()));
		HttpConnectionParams.setSoTimeout(httpParams, (int) TimeUnit.SECONDS.toMillis(cfg.getSocketTimeout()));
		DefaultHttpClient client = new DefaultHttpClient(httpParams);
		BufferedReader bReader = null;
		InputStream inStream = null;

		HttpGet get = new HttpGet(url);
		HttpResponse response;
		try {
			response = client.execute(get);
			inStream = response.getEntity().getContent();
			bReader = new BufferedReader(new InputStreamReader(inStream));
			StringBuilder strBuilder = new StringBuilder();
			String line;
			while (null != (line = bReader.readLine())) {
				strBuilder.append(line);
			}
			logger.info("Return Msg From BS Yeahmobi: " + strBuilder.toString());
			return String.valueOf(response.getStatusLine().getStatusCode());
		} catch (ClientProtocolException e) {
			logger.error("error:", e);
		} catch (IOException e) {
			logger.error("error:", e);
		}

		return "404";
	}

	public static void yeahMobiCallBack(String url) {
		HttpParams httpParams = new BasicHttpParams();
		HttpConnectionParams.setConnectionTimeout(httpParams, (int) TimeUnit.SECONDS.toMillis(cfg.getConnTimeout()));
		HttpConnectionParams.setSoTimeout(httpParams, (int) TimeUnit.SECONDS.toMillis(cfg.getSocketTimeout()));
		DefaultHttpClient client = new DefaultHttpClient(httpParams);
		BufferedReader bReader = null;
		InputStream inStream = null;

		HttpGet get = new HttpGet(url);
		HttpResponse response;
		try {
			response = client.execute(get);
			inStream = response.getEntity().getContent();
			bReader = new BufferedReader(new InputStreamReader(inStream));
			StringBuilder strBuilder = new StringBuilder();
			String line;
			while (null != (line = bReader.readLine())) {
				strBuilder.append(line);
			}
			logger.info("Return Msg From BS Yeahmobi: " + strBuilder.toString());
			String code = String.valueOf(response.getStatusLine().getStatusCode());
			if (!Strings.isNullOrEmpty(code)) {
				if ("200".equals(code)) {
					logger.info("BS Yeahmobi Api [" + url + "] CallBack Succeed.");
				} else {
					logger.info("BS Yeahmobi Api [" + url + "] CallBack Returned With Code [" + code + "].");
				}
			}
		} catch (ClientProtocolException e) {
			logger.error("error:", e);
		} catch (IOException e) {
			logger.error("error:", e);
		}
	}

	@SuppressWarnings("deprecation")
	public static void yeahMobiSSLCallBack(String url) {
		HttpGet get = new HttpGet(url);
		HttpParams httpParams = new BasicHttpParams();
		try {
			KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
			trustStore.load(null, null);

			SSLSocketFactory sf = new YeahmobiSSLSocketFactory(trustStore);
			sf.setHostnameVerifier(SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);

			HttpProtocolParams.setVersion(httpParams, HttpVersion.HTTP_1_1);
			HttpProtocolParams.setContentCharset(httpParams, HTTP.UTF_8);

			HttpConnectionParams.setConnectionTimeout(httpParams, (int) TimeUnit.SECONDS.toMillis(cfg.getConnTimeout()));
			HttpConnectionParams.setSoTimeout(httpParams, (int) TimeUnit.SECONDS.toMillis(cfg.getSocketTimeout()));

			SchemeRegistry registry = new SchemeRegistry();
			registry.register(new Scheme("http", PlainSocketFactory.getSocketFactory(), 80));
			registry.register(new Scheme("https", sf, 443));

			ClientConnectionManager ccm = new ThreadSafeClientConnManager(httpParams, registry);

			DefaultHttpClient client = new DefaultHttpClient(ccm, httpParams);

			HttpResponse response = client.execute(get);
			InputStream inStream = response.getEntity().getContent();
			BufferedReader bReader = new BufferedReader(new InputStreamReader(inStream));
			StringBuilder strBuilder = new StringBuilder();
			String line;
			while (null != (line = bReader.readLine())) {
				strBuilder.append(line);
			}
			logger.info("Return Msg From BS Yeahmobi: " + strBuilder.toString());
			String code = String.valueOf(response.getStatusLine().getStatusCode());
			if (!Strings.isNullOrEmpty(code)) {
				if ("200".equals(code)) {
					logger.info("BS Yeahmobi Api [" + url + "] SSL-CallBack Succeed.");
				} else {
					logger.info("BS Yeahmobi Api [" + url + "] SSL-CallBack Returned With Code [" + code + "].");
				}
			}
		} catch (KeyStoreException e) {
			logger.error("error:", e);
		} catch (NoSuchAlgorithmException e) {
			logger.error("error:", e);
		} catch (CertificateException e) {
			logger.error("error:", e);
		} catch (IOException e) {
			logger.error("error:", e);
		} catch (KeyManagementException e) {
			logger.error("error:", e);
		} catch (UnrecoverableKeyException e) {
			logger.error("error:", e);
		}
	}

	static String urlString = "http://127.0.0.1:8080/datasystem-realquery/report?report_param={%22settings%22:{%22report_id%22:%228412b4adf34ae00d9559e212c72ce57c%22,%22return_format%22:%22json%22,%22data_source%22:%22ymds_druid_datasource%22,%22time%22:{%22start%22:1401321600,%22end%22:1401408000,%22timezone%22:0},%22pagination%22:{%22size%22:50,%22page%22:0}},%22group%22:[%22click_time%22,%22conv_time%22,%22time_diff%22,%22aff_id%22,%22aff_sub1%22,%22offer_id%22,%22transaction_id%22,%22country%22,%22conv_ip%22,%22click_ip%22],%22data%22:[%22cr%22],%22filters%22:{%22$and%22:{%22aff_manager%22:{%22$in%22:[%2231%22]}}}}";
	static String uri = "http://127.0.0.1:9099/xchangeRates";
	// static String uri = "http://127.0.0.1:8080/datasystem-realquery/report";
	// static String parameter =
	// "{\"settings\":{\"report_id\":\"8412b4adf34ae00d9559e212c72ce57c\",\"return_format\":\"json\",\"data_source\":\"ymds_druid_datasource\",\"time\":{\"start\":1401321600,\"end\":1401408000,\"timezone\":0},\"pagination\":{\"size\":50,\"page\":0}},\"group\":[\"click_time\",\"conv_time\",\"time_diff\",\"aff_id\",\"aff_sub1\",\"offer_id\",\"transaction_id\",\"country\",\"conv_ip\",\"click_ip\"],\"data\":[\"cr\"],\"filters\":{\"$and\":{\"aff_manager\":{\"$in\":[\"31\"]}}}}";
	static String parameter = "{\"query_type\":\"all\",\"return_format\":\"json\",\"query_id\":\"88e771bc-a11-44dd-afa2-9fcc056e9eeb\",\"colums\":[\"currency_from\",\"currency_to\",\"rate_from_to\",\"rate_usd_to\"]}";
	static String url2 = "http://127.0.0.1:8080/datasystem-realquery/report?report_param={\"settings\":{\"report_id\":\"8412b4adf34ae00d9559e212c72ce57c\",\"return_format\":\"json\",\"data_source\":\"ymds_druid_datasource\",\"time\":{\"start\":1401321600,\"end\":1401408000,\"timezone\":0},\"pagination\":{\"size\":50,\"page\":0}},\"group\":[\"click_time\",\"conv_time\",\"time_diff\",\"aff_id\",\"aff_sub1\",\"offer_id\",\"transaction_id\",\"country\",\"conv_ip\",\"click_ip\"],\"data\":[\"cr\"],\"filters\":{\"$and\":{\"aff_manager\":{\"$in\":[\"31\"]}}}}";

	public static void main(String[] args) throws UnsupportedEncodingException {
		Injector injector = Guice.createInjector(new QueryConfigModule());
		QueryConfig cfg = injector.getInstance(QueryConfig.class);
		System.out.println("XchangeSyncAPI : " + cfg.getXchangeSyncAPI());
		System.out.println("getXchangeSyncQueryType : " + cfg.getXchangeSyncQueryType());
		System.out.println("getXchangeSyncReturnFormat : " + cfg.getXchangeSyncReturnFormat());
		System.out.println("getXchangeSyncColums : " + Arrays.toString(cfg.getXchangeSyncColums().split(",")));
		XchangeQueryParam param = new XchangeQueryParam();
		// param.setQuery_id(DigestUtils.md5Hex(String.valueOf(new
		// Date().getTime())));
		param.setQuery_id(UUID.randomUUID().toString());
		param.setQuery_type(cfg.getXchangeSyncQueryType());
		param.setReturn_format(cfg.getXchangeSyncReturnFormat());
		param.setColums(cfg.getXchangeSyncColums().split(","));
		System.out.println(parameter);
		System.out.println(new Gson().toJson(param));
		try {
			List<XCHANGE_RATE_BASE> xchange_rate_bases = getXchangeRateList();
			if (null != xchange_rate_bases && xchange_rate_bases.size() > 0) {
				System.out.println("xchange_rate_bases.size = " + xchange_rate_bases.size());
				for (XCHANGE_RATE_BASE xchange_rate_base : xchange_rate_bases) {
					System.out.println("from_to = " + xchange_rate_base.getCurrency_from() + xchange_rate_base.getCurrency_to());
				}
			} else {
				System.out.println("none found.");
			}
		} catch (ReportParserException | IOException e) {
			e.printStackTrace();
		}
		String tmp = URLDecoder.decode(urlString, "UTF-8");
		// System.out.println(tmp.charAt(63));
		// System.out.println(NetService.yeahmobiCallBack(URLDecoder.decode(urlString,
		// "UTF-8")));
		// System.out.println(NetService.yeahmobiCallBack(tmp));
		// System.out.println(NetService.yeahmobiCallBack3(uri + "?params=" +
		// URLEncoder.encode(parameter, "UTF-8")));
	}
}

class YeahmobiSSLSocketFactory extends SSLSocketFactory {
	SSLContext sslContext = SSLContext.getInstance("TLS");

	public YeahmobiSSLSocketFactory(KeyStore truststore) throws NoSuchAlgorithmException, KeyManagementException, KeyStoreException, UnrecoverableKeyException {
		super(truststore);

		TrustManager tm = new X509TrustManager() {
			public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
			}

			public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
			}

			public X509Certificate[] getAcceptedIssuers() {
				return null;
			}
		};

		sslContext.init(null, new TrustManager[] { tm }, null);
	}

	@Override
	public Socket createSocket(Socket socket, String host, int port, boolean autoClose) throws IOException, UnknownHostException {
		return sslContext.getSocketFactory().createSocket(socket, host, port, autoClose);
	}

	@Override
	public Socket createSocket() throws IOException {
		return sslContext.getSocketFactory().createSocket();
	}
}
