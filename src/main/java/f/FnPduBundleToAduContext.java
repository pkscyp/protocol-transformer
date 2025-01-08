package f;

import java.util.Map;
import java.util.Optional;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import i.AduBundleContext;
import i.PduBundleContext;
import pipeline.messages.PduBundle;
import u.JSTransformerHelper;

public class FnPduBundleToAduContext extends RichMapFunction<PduBundle,AduBundleContext>{

	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(FnPduBundleToAduContext.class);
	
	public FnPduBundleToAduContext() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public AduBundleContext map(PduBundle record) throws Exception {
		PduBundleContext pctx = new PduBundleContext(record);
		AduBundleContext actx = new AduBundleContext(pctx);
		try {
			Object js = JSON.parse(record.getPayload());
			if(js instanceof JSONArray) {
				JSONArray jarr = (JSONArray)js;
				for(int i=0;i<jarr.size();i++) {
					JSONObject jspdu = jarr.getJSONObject(i);
					String pdu_name = jspdu.keySet().iterator().next();
					Map<String,Object> valueMap=null;
					if(jspdu.get(pdu_name) instanceof JSONArray) {
						Optional<Map<String,Object>> vmap = JSTransformerHelper.with().convertPdu(pdu_name+"_toMap",jspdu.getJSONArray(pdu_name).toString());
						if(vmap.isPresent())
							valueMap = vmap.get();
						else
							continue;
					}else {
						valueMap = jspdu.getJSONObject(pdu_name);
					}
					
					JSTransformerHelper.with().invokeFunction(pdu_name, valueMap,actx);
					
				}
			}
		}catch(Exception ex) {
			logger.warn("eaception occured for {}",record.getPayload());
		}
		
		
		return actx;
	}

}
