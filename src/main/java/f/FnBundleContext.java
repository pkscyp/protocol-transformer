package f;

import java.util.Map;
import java.util.Optional;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import i.GenericPdu;
import i.PduBundleContext;
import pipeline.messages.PduBundle;
import u.JSTransformerHelper;

public class FnBundleContext extends RichMapFunction<PduBundle,PduBundleContext>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(BuildPduContext.class);
	
	private static ObjectMapper mapper = new ObjectMapper();
	
	private static TypeReference<Map<String, Object>> _mapTypeInfo = new TypeReference<Map<String, Object>>(){};
	
	public FnBundleContext() {
		
	}

	@Override
	public PduBundleContext map(PduBundle record) throws Exception {
		PduBundleContext ctx = new PduBundleContext(record);
				       
		try {
			JsonNode  json_bundle = mapper.readTree(record.getPayload());
			Map<String,Object> pduinfo = null;
			if (json_bundle.isArray()) {
					
				for( JsonNode jn: json_bundle) {
					String name = jn.fieldNames().next();
					JsonNode value = jn.get(name);
					if(value.isArray()) {
						Optional<Map<String,Object>> popt = JSTransformerHelper.with().convertPdu(name+"_toMap",value.toPrettyString());
						if(popt.isPresent()) {
							pduinfo = popt.get();
						}else {
							continue;
						}
					}else {
						pduinfo  = mapper.convertValue(jn.get(name),_mapTypeInfo);
					}
								
					ctx.addPdu(new GenericPdu(name,pduinfo));
				}	
			}
			ctx.sortPdusByTime();
			}catch(Exception ex) {
				logger.warn("eaception occured for {}",record.getPayload());
			}
		
		return ctx;
	}

}
