package p.s;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerStats;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TopicMessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static i.MetricNames.*;
;
public class PulsarSourceReader<T> implements SourceReader<T, NoSplit>,ReaderMessageAckable {

	private static final Logger logger = LoggerFactory.getLogger(PulsarSourceReader.class);
	NoSplit currentSplit=null;
	PulsarSourceConfigBuilder config;
	Integer count;
	CompletableFuture<Void> availability = CompletableFuture.completedFuture(null);
	SourceReaderContext context;
	Boolean nowStop=false;
	PulsarClient client=null;
	Consumer<String> consumer=null;
	AtomicInteger msgackTracker = new AtomicInteger(0);
	@SuppressWarnings("rawtypes")
	final BiFunction<Message<String>, Integer,T> converter;
	Integer max_fetch_count;
	
	
	private final Integer myId;
	private final BlockingQueue<Message> queue ;
	private final BlockingQueue<MessageId> ackqueue ;
	private final PulsarSourceFactoryManager<T> factory;
	private SourceReaderMetricGroup metricGroup;
	
	public PulsarSourceReader(SourceReaderContext context, PulsarSourceFactoryManager<T> factory) {
		this.config=factory.getConfiguration();
		this.context=context;
		this.converter = factory.getConvertor();
		this.max_fetch_count = this.config.get(config.PULSAR_MAX_FETCH_RECORDS);
		this.myId = factory.getNewObjectId();
		this.factory=factory;
		this.queue = new ArrayBlockingQueue<>(max_fetch_count+1);
		this.ackqueue =  new ArrayBlockingQueue<>(max_fetch_count+1);
		this.metricGroup = context.metricGroup();
		logger.info("**** Max Fetch Size {} ***",this.max_fetch_count);
	}

	@Override
	public void close() throws Exception {
		logger.info("Close Reader");
		if(consumer!=null)
			consumer.close();
		if(client!=null)
			client.close();
		factory.removeReader(myId);
	}

	@Override
	public void addSplits(List<NoSplit> splitsList) {
		logger.info("addSplits {} ",splitsList);
		currentSplit = splitsList.get(0);
		nowStop = currentSplit.getNowStop();
		  availability.complete(null);
	}

	@Override
	public CompletableFuture<Void> isAvailable() {
//	logger.info(" {} isAvailable acks={} queue={} ",myId,ackqueue.size(),queue.size());
		while(!ackqueue.isEmpty()) {
			MessageId mid=ackqueue.poll();
			if(mid!=null) {
				try {
					consumer.acknowledge(mid);
					logger.debug("Received event for myId={} pulsarId={}",myId,mid.toString());
					msgackTracker.getAndDecrement();
				} catch (PulsarClientException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		if(queue.isEmpty()) {
			try {
				
				for(int i=0;i<max_fetch_count;i++) {
					
						Message<String> m = consumer.receive(10, TimeUnit.MILLISECONDS);
						if(m==null) break;
						queue.offer(m);
						m.release();
						logger.debug("{} Loaded {}",myId, m.getMessageId());
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return availability;
	}

	@Override
	public void notifyNoMoreSplits() {
		logger.debug("notifynomoresplits");
		
	}

	
	@Override
	public InputStatus pollNext(ReaderOutput<T> out) throws Exception {
	//	logger.info(" {} pollnext pending acks={}",(currentSplit!=null)?currentSplit.getId():-1,msgackTracker.get());
		if(currentSplit!=null && !nowStop) {
			count=max_fetch_count;
			Message<String> msg = queue.poll();//consumer.receive(10, TimeUnit.MILLISECONDS);		
			while(msg !=null && count>0) {
				--count;
				msgackTracker.getAndIncrement();
				out.collect(this.converter.apply(msg,myId));
				msg = queue.poll();
				
			}
			
				return queue.isEmpty()?InputStatus.NOTHING_AVAILABLE: InputStatus.MORE_AVAILABLE;

		}else {
			if(availability.isDone()) {
				 availability = new CompletableFuture<Void>();
			     if(!nowStop) context.sendSplitRequest();
			}
			logger.info("stop={} pendingacks={}",nowStop,msgackTracker.get());
			if(nowStop && msgackTracker.get() == 0)
				return InputStatus.END_OF_INPUT;
			return InputStatus.NOTHING_AVAILABLE;
		}
		
	}

	@Override
	public List<NoSplit> snapshotState(long arg0) {
		logger.info("snapshotstate");
		return Collections.emptyList();
	}
	
	private void createConsumer() {
		try {
			List<String> topics = Arrays.asList(config.get(PulsarSourceConfigBuilder.PULSAR_TOPIC_NAMES).split(";"));
			consumer = client.newConsumer(Schema.STRING)
					.subscriptionMode(config.get(config.PULSAR_SUBSCRIPTION_MODE))
					.subscriptionName(config.get(config.PULSAR_SUBSCRIPTION_NAME))
					.receiverQueueSize(config.get(config.PULSAR_RECEIVER_QUEUE_SIZE))
					.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
					.topics(topics)
					.subscriptionType(config.get(PulsarSourceConfigBuilder.PULSAR_SUBSCRIPTION_TYPE))
					.ackTimeout(config.get(config.PULSAR_ACK_TIMEOUT_MILLIS), TimeUnit.MILLISECONDS)					
					.keySharedPolicy(KeySharedPolicy.autoSplitHashRange())
					.maxTotalReceiverQueueSizeAcrossPartitions(config.get(config.PULSAR_MAX_TOTAL_RECEIVER_QUEUE_SIZE_ACROSS_PARTITIONS))
					.poolMessages(false)
					.subscribe();
			logger.info("Consumer is read ...");
			exposeConsumerMetrics(consumer);
			
		}catch (Exception e) {
			throw new FlinkRuntimeException(e.getMessage());
		}
	}

	private void exposeConsumerMetrics(Consumer<String> consumer) {

          /**  String consumerIdentity = factory.getReaderGlobalId(myId);


            MetricGroup group =
                    metricGroup
                            .addGroup(PULSAR_CONSUMER_METRIC_NAME)
                            .addGroup(consumer.getTopic())
                            .addGroup(consumerIdentity);
            
            
            group.gauge(NUM_MSGS_RECEIVED, consumer.getMsgOutCounter());
            group.gauge(NUM_ACKS_SENT, stats::getNumAcksSent);
            group.gauge(NUM_ACKS_FAILED, stats::getNumAcksFailed);
            group.gauge(NUM_RECEIVE_FAILED, stats::getNumReceiveFailed);
            group.gauge(TOTAL_MSGS_RECEIVED, stats::getTotalMsgsReceived);
            group.gauge(TOTAL_RECEIVED_FAILED, stats::getTotalReceivedFailed);
            group.gauge(TOTAL_ACKS_SENT, stats::getTotalAcksSent);
            group.gauge(TOTAL_ACKS_FAILED, stats::getTotalAcksFailed);
            group.gauge(MSG_NUM_IN_RECEIVER_QUEUE, stats::getMsgNumInReceiverQueue);
            ***/
     
    }
	
	@Override
	public void start() {
		String broker_url = config.get(PulsarSourceConfigBuilder.PULSAR_SERVICE_URL);
		if(broker_url == null)
			throw new FlinkRuntimeException(PulsarSourceConfigBuilder.PULSAR_SERVICE_URL.key());
		count=max_fetch_count;
		try {
			client = PulsarClient.builder()
					.serviceUrl(broker_url)
					.listenerThreads(config.get(PulsarSourceConfigBuilder.PULSAR_NUM_LISTENER_THREADS))
					.statsInterval(config.get(PulsarSourceConfigBuilder.PULSAR_STATS_INTERVAL_SECONDS), TimeUnit.SECONDS)				
					.ioThreads(config.get(PulsarSourceConfigBuilder.PULSAR_NUM_IO_THREADS))
					.build();
			createConsumer();
			context.sendSplitRequest();
			factory.addReader(myId, this);
		} catch (Exception e) {
			throw new FlinkRuntimeException(e.getMessage());
		}
		
	}
	
	@Override
	public void acknowledge(String msgId,String topicName)  {
		if(consumer!=null) {
			try {
				byte[] id = Base64.getDecoder().decode(msgId);
				MessageId mid = MessageId.fromByteArrayWithTopic(id,topicName);	
			//	logger.info("MessageId Class {}",mid.getClass().getCanonicalName());
				ackqueue.put(mid);
				
				
			} catch (Exception e) {
				logger.error("ack_error",e);
			}
		}
		
	}
	
	

}
