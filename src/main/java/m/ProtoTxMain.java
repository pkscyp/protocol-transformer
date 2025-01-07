package m;

import java.io.IOException;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import u.*;
import i.*;
import m.TestSourceBoot.PulsarMessageConverter;
import p.s.*;
import pipeline.messages.AduMessage;
import pipeline.messages.AduMessageToPulsarMessage;
import pipeline.messages.PduBundle;
import pipeline.messages.PulsarMessageToPduBundle;
import a.*;
import f.*;

public class ProtoTxMain {

	private static final Logger logger = LoggerFactory.getLogger(ProtoTxMain.class);
	
	public ProtoTxMain() {
		
	}
	
	public static void main(String[] args) throws IOException {
		Configuration conf = FlinkSettings.createConfiguration();
		JSTransformerHelper.with();
		PulsarSourceConfigBuilder pb = new PulsarSourceConfigBuilder();
		pb.set(conf).topicNames("persistent://public/default/om500_bundle")
		.subscriptionName("PduBundleReader")
		.subscriptionMode("Durable");
		
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
		final PulsarSourceFactoryManager<PduBundle> factory =  
				                   new PulsarSourceFactoryManager<PduBundle>(
             		   new PulsarMessageToPduBundle()
             		   ,PduBundle.class,
             		  pb);
		final PulsarSource<PduBundle> source  = new PulsarSource<PduBundle>(factory);
		env.fromSource(source, WatermarkStrategy.noWatermarks(), "Pulsar Key Shared Reader")
			.keyBy(x -> x.getMessageKey())
			.map( new FnBundleContext())
			.map( new FnPduToAduContext())
			.flatMap(new FnFlatMapAduContext())
			.map(new FnPushToPulsar<AduMessage>(new AduMessageToPulsarMessage(), pb))
			.map(new FnAckDispatcher(factory))
			.filter(x -> false);
		try {
			
			env.execute("Pulsar KeyShared Source");
			
			
		} catch (Exception e) {
			logger.error("E",e);
		}

	}

}
