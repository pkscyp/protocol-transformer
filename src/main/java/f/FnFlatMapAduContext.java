package f;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import i.AduBundleContext;
import pipeline.messages.AduMessage;

import com.alibaba.fastjson2.JSON;

public class FnFlatMapAduContext extends RichFlatMapFunction<AduBundleContext,AduMessage>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public FnFlatMapAduContext() {
		
	}

	@Override
	public void flatMap(AduBundleContext ctx, Collector<AduMessage> out) throws Exception {
		Integer maxAdus = ctx.ads.size();
		if(maxAdus<=0) {
			AduMessage m = new AduMessage(ctx).setMaxAduInBundle(maxAdus);
			out.collect(m);
		}else {
			for(int i=0;i<maxAdus;i++) {
				AduMessage m = new AduMessage(ctx)
						.setMaxAduInBundle(maxAdus)
						.addTopics("persistent://public/default/adus")
						.setPayload(JSON.toJSONString(ctx.ads.get(i)));
				out.collect(m);
				
			}
		}
		
		
	}

}
