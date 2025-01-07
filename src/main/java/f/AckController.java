package f;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import m.Boot.AduPayloadRecord;
import p.s.PulsarSourceFactoryManager;
import pipeline.messages.AduMessage;

public class AckController extends RichMapFunction<AduPayloadRecord,AduPayloadRecord>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(AckController.class);
	
	private ConcurrentHashMap<String,Integer> bundle_tracker=new ConcurrentHashMap<>();

	public AckController() {
		
	}

	@Override
	public AduPayloadRecord map(AduPayloadRecord rec) throws Exception {
		
	 Integer maxAdu = bundle_tracker.computeIfAbsent(rec.bid, k -> 1);
	 //logger.info(" bundle Tracker {} {} >= {} ",rec.bid,maxAdu,rec.maxAduCount);
	 if(maxAdu >= rec.maxAduCount ) {
		 logger.info(" Time to ack the Bundle {} {} >= {} ", rec.bid,maxAdu,rec.maxAduCount);
		 bundle_tracker.remove(rec.bid);
	 }else {
		 bundle_tracker.put(rec.bid, ++maxAdu);
	 }
		
		return rec;
	}

}
