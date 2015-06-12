package com.yeahmobi.datasystem.query.akka.http;
/**
 * Created by yangxu on 5/5/14.
 */

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.common.primitives.Bytes;
import com.ning.http.client.AsyncHandler;
import com.ning.http.client.HttpResponseBodyPart;
import com.ning.http.client.HttpResponseHeaders;
import com.ning.http.client.HttpResponseStatus;
import com.yeahmobi.datasystem.query.exception.ReportRuntimeException;

public class DruidAsyncHandler implements AsyncHandler<Boolean> {
	private boolean isSuccess = true;
	
	List<Byte> byteCache = new ArrayList<>();
	final static int trunkSize = 8192;

    @Override
    public void onThrowable(Throwable throwable) {

    }

    final BodyConsumer consumer;

    public DruidAsyncHandler(BodyConsumer consumer) {
        this.consumer = consumer;
    }

    public STATE onBodyPartReceived(final HttpResponseBodyPart content)
            throws Exception {
    	
    	byte[] bytes = content.getBodyPartBytes();
    	for(byte b : bytes){
    		byteCache.add(b);
    	}
    	
    	if(byteCache.size() >= trunkSize){
    		consumer.write(Bytes.toArray(byteCache));
    		byteCache.clear();
    		consumer.tryParse();
    		consumer.trySend(false);
    	}

        return STATE.CONTINUE;
    }

    public STATE onStatusReceived(final HttpResponseStatus status)
            throws Exception {
    	logger.debug("the status from druid is " + status.getStatusCode());
    	if(status.getStatusCode() != 200){
    		this.isSuccess = false;
    		throw new ReportRuntimeException("Error occured: the druid result exceeds the max limit[500000] or other reason");
    	}
        return STATE.CONTINUE;
    }

    public STATE onHeadersReceived(final HttpResponseHeaders headers)
            throws Exception {
        return STATE.CONTINUE;
    }

    public Boolean onCompleted() throws Exception {
    	if(isSuccess){
    		if(byteCache.size() > 0){
    			consumer.write(Bytes.toArray(byteCache));
    			byteCache.clear();
    			consumer.tryParse();    			
    		}
    		if (!consumer.hasSent()) {
    			consumer.trySend(true);
    		}
    		
    		consumer.tryClose();
    		
    		return consumer.tryCache();
    	}
    	return false;
    }
    
    private final static Logger logger = Logger.getLogger(DruidAsyncHandler.class);
}

