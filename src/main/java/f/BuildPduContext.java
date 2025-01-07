package f;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import i.GenericPdu;
import i.PduContext;
import m.Boot.AtomicPdus;
import m.Boot.PayloadRecord;
import u.JSTransformerHelper;

public class BuildPduContext extends RichMapFunction<PayloadRecord,PduContext>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(BuildPduContext.class);
	
	private static ObjectMapper mapper = new ObjectMapper();
	
	private static TypeReference<Map<String, Object>> _mapTypeInfo = new TypeReference<Map<String, Object>>(){};
	
	private static TypeReference<List<Object>> _listTypeInfo = new TypeReference<List<Object>>(){};
	
	
	public BuildPduContext() {
		
	}

	@Override
	public PduContext map(PayloadRecord record) throws Exception {
		PduContext pctx = new PduContext("om500",record.imei);
		try {
		JsonNode  json_bundle = mapper.readTree(record.payload);
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
							
				pctx.addPdu(new GenericPdu(name,pduinfo));
			}	
		}
		pctx.sortPdusByTime();
		}catch(Exception ex) {
			logger.warn("eaception occured for {}",record.payload);
		}
		return pctx;
	}

}
