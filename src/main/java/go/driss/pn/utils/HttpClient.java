package go.driss.pn.utils;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.GzipDecompressingEntity;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.config.SocketConfig;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpClient {

	private static Logger logger = LoggerFactory.getLogger(HttpClient.class);

	private static CloseableHttpClient httpClient;

	static {
		if (httpClient == null) {
			RequestConfig requestConfig = RequestConfig.custom()
					.setSocketTimeout(3000).setConnectTimeout(500)
					.setConnectionRequestTimeout(500).build();
			PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
			connManager.setMaxTotal(1500);
			connManager.setDefaultMaxPerRoute(1000);
			SocketConfig socketConfig = SocketConfig.custom()
					.setTcpNoDelay(true).setSoReuseAddress(true)
					.setSoTimeout(3000).setSoKeepAlive(true).build();
			connManager.setDefaultSocketConfig(socketConfig);
			httpClient = HttpClients.custom().setConnectionManager(connManager)
					.setDefaultRequestConfig(requestConfig).build();
		}
	}

	public static String get(String url) {
		HttpGet request = new HttpGet(url);
		try {
			HttpResponse response = httpClient.execute(request);
			if (response.getStatusLine().getStatusCode() != 200) {
				return null;
			}
			final Header contentencoding = response
					.getFirstHeader("Content-Encoding");
			HttpEntity entity = response.getEntity();
			String encoding = ("" + contentencoding)
					.toLowerCase();
			if (encoding.indexOf("gzip") > 0) {
				entity = new GzipDecompressingEntity(entity);
			}
			String content = new String(EntityUtils.toByteArray(entity), "UTF-8");
			return content;
		} catch (Exception e) {
			return null;
		}finally {
			request.releaseConnection();
		}
	}

	public static String post(String url, Map<String, String> header,Map<String, String> params, int timeOut) {
		HttpPost httppost = new HttpPost(url);
		try{
			if(header != null){
				Iterator<String>iterator = header.keySet().iterator();
				while(iterator.hasNext()){
					String key = iterator.next();
					String value = header.get(key);
					httppost.addHeader(key, value);
				}
			}
			List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>();
			for (String key : params.keySet()) {
				nameValuePairs.add(new BasicNameValuePair(key, params.get(key)));
			}
			httppost.setEntity(new UrlEncodedFormEntity(nameValuePairs, "UTF-8"));
			HttpResponse response = httpClient.execute(httppost);
			if (response.getStatusLine().getStatusCode() != 200) {
				return null;
			}
			final Header contentencoding = response
					.getFirstHeader("Content-Encoding");
			HttpEntity entity = response.getEntity();
			String encoding = ("" + contentencoding)
					.toLowerCase();
			if (encoding.indexOf("gzip") > 0) {
				entity = new GzipDecompressingEntity(entity);
			}
			String content = new String(EntityUtils.toByteArray(entity), "UTF-8");
			return content;
		}catch(Exception e){
			return null;
		}finally {
			httppost.releaseConnection();
		}
	}

	public static String post(String url, Map<String, Object> params) {
		HttpPost httppost = new HttpPost(url);
		try{
			List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>();
			for (String key : params.keySet()) {
				nameValuePairs.add(new BasicNameValuePair(key, String.valueOf(params.get(key))));
			}
			httppost.setEntity(new UrlEncodedFormEntity(nameValuePairs, "UTF-8"));
			HttpResponse response = httpClient.execute(httppost);
			if (response.getStatusLine().getStatusCode() != 200) {
				return null;
			}
			final Header contentencoding = response
					.getFirstHeader("Content-Encoding");
			HttpEntity entity = response.getEntity();
			String encoding = ("" + contentencoding)
					.toLowerCase();
			if (encoding.indexOf("gzip") > 0) {
				entity = new GzipDecompressingEntity(entity);
			}
			String content = new String(EntityUtils.toByteArray(entity), "UTF-8");
			return content;
		}catch(Exception e){
			return null;
		}finally {
			httppost.releaseConnection();
		}
	}

	public static String post(String url, Map<String, String> header, String body, int timeOut) {
		HttpPost httppost = new HttpPost(url);
		try{
			if(header != null){
				Iterator<String>iterator = header.keySet().iterator();
				while(iterator.hasNext()){
					String key = iterator.next();
					String value = header.get(key);
					httppost.addHeader(key, value);
				}
			}
//			httppost.addHeader("Content-Type","application/json; charset=utf-8");
			StringEntity s = new StringEntity(body, Charset.forName("UTF-8"));
			s.setContentType("application/json");
			httppost.setEntity(s);
			HttpResponse response = httpClient.execute(httppost);
			if (response.getStatusLine().getStatusCode() != 200) {
				return null;
			}
			final Header contentencoding = response
					.getFirstHeader("Content-Encoding");
			HttpEntity entity = response.getEntity();
			String encoding = ("" + contentencoding)
					.toLowerCase();
			if (encoding.indexOf("gzip") > 0) {
				entity = new GzipDecompressingEntity(entity);
			}
			String content = new String(EntityUtils.toByteArray(entity), "UTF-8");
			return content;
		}catch(Exception e){
			return null;
		}finally {
			httppost.releaseConnection();
		}
	}

}
