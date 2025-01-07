package f;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import m.Boot.AduPayloadRecord;
import p.s.PulsarSourceFactoryManager;
import pipeline.messages.AduMessage;

public class FnAckDispatcher<T> extends RichMapFunction<AduMessage,AduMessage>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(FnAckDispatcher.class);
	
	private Map<String,Integer> bundle_tracker=new HashMap<>();
	final PulsarSourceFactoryManager<T> factory;
	
	public FnAckDispatcher(PulsarSourceFactoryManager<T> factory) {
		this.factory = factory;
		
	}

	@Override
	public AduMessage map(AduMessage rec) throws Exception {
		
	 Integer maxAdu = bundle_tracker.computeIfAbsent(rec.getPulsarMessageId(), k -> 1);
	 //logger.info(" bundle Tracker {} {} >= {} ",rec.bid,maxAdu,rec.maxAduCount);
	 if(maxAdu >= rec.getMaxAduInBundle() ) {		 
		 logger.debug(" Sending Ack {} {} >= {} ",rec.getPulsarMessageId(),
				 maxAdu,rec.getMaxAduInBundle());
		 factory.ackMessage(rec.getReaderId(), rec.getPulsarMessageId(),rec.getSourceTopic());
		 bundle_tracker.remove(rec.getPulsarMessageId());
	 }else {
		 bundle_tracker.put(rec.getPulsarMessageId(), ++maxAdu);
	 }
		
		return rec;
	}
}
