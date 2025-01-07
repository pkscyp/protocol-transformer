package p.s;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import i.Stopable;


public class PulsarSourceEnumerator implements SplitEnumerator<NoSplit, Void>,Stopable{

	private static final Logger logger = LoggerFactory.getLogger(PulsarSourceEnumerator.class);
	
	SplitEnumeratorContext<NoSplit> enumContext;
	final AtomicInteger controller = new AtomicInteger(100);
	List<Integer> readers = new ArrayList<>();
	final PulsarSourceFactoryManager<?> factory;
	Integer myId;
	private volatile Boolean initiateStop=false;
	
	
	public PulsarSourceEnumerator(SplitEnumeratorContext<NoSplit> enumContext,PulsarSourceFactoryManager factory) {
		this.enumContext=enumContext;
		this.factory = factory;
		this.myId=factory.getNewObjectId();
	}


	public void setSplitEnumeratorContext(SplitEnumeratorContext<NoSplit> enumContext) {
		this.enumContext=enumContext;
	}

	@Override
	public void addReader(int id) {
		logger.info("Add Reader {}",id);
		readers.add(id);
	}

	@Override
	public void addSplitsBack(List<NoSplit> splitlist, int taskid) {
		logger.info(" add split back {} {}",splitlist,taskid);
		
	}

	@Override
	public void close() throws IOException {
		logger.info("CLose enumerator ");
		factory.removeEnumerator(myId);
	}

	@Override
	public void handleSplitRequest(int taskid, String hostname) {
		logger.info(" handle Split request taskid={}, hostname={}",taskid,hostname);
		Integer i = controller.decrementAndGet();
		if(i>0) {
			enumContext.assignSplit(new NoSplit(taskid), taskid);
		}else {
			enumContext.assignSplit(new NoSplit(taskid,true), taskid);
		}
	}

	@Override
	public Void snapshotState(long arg0) throws Exception {
		
		return null;
	}

	private Integer isShutDownTime() {
		if(initiateStop) return 0;
		return controller.decrementAndGet();
	}
	
	@Override
	public void start() {
		logger.info("Start Enumerator");
		enumContext.callAsync(this::isShutDownTime, (v,t)-> {
			if(v<=0) {
				readers.stream().forEach((taskid) -> enumContext.assignSplit(new NoSplit(taskid,true), taskid));
			}
		},0,2000);
		factory.addEnumerator(myId, this);
	}


	@Override
	public void stop() {
		this.initiateStop=true;;
		
	}
	
	

}
