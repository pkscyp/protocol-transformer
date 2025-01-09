package grpc;

import pdutx.*;
import u.JSTransformerHelper;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import i.AduContext;
import io.grpc.stub.StreamObserver;

public class TxPduServiceImpl extends TxServiceGrpc.TxServiceImplBase{

	public TxPduServiceImpl() {
		
	}
	
	@Override
	public void txPdu(TxPduRequest request,
	        StreamObserver<pdutx.TxPduResponse> responseObserver) {

		
		String msgId = request.getMsgId();
		String payload = request.getPduBundleJson();
		Object js = JSON.parse(payload);
		JSONArray result = new JSONArray();
		AduContext aduContext = new AduContext(msgId);
		
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
				jspdu.put(pdu_name, valueMap);
				
			}
			Collections.sort(jarr,(a,b) -> {
				JSONObject ai = (JSONObject)a;
				JSONObject av = ai.getJSONObject(ai.keySet().iterator().next());
				JSONObject bi = (JSONObject)b;
				JSONObject bv = bi.getJSONObject(bi.keySet().iterator().next());
				return av.getInteger("ts").compareTo(bv.getInteger("ts"));
			});
			for(int i=0;i<jarr.size();i++) {
				JSONObject jspdu = jarr.getJSONObject(i);
				String pdu_name = jspdu.keySet().iterator().next();
				Map<String,Object> valueMap=jspdu.getJSONObject(pdu_name);
				JSTransformerHelper.with().invokeFunction(pdu_name, valueMap,aduContext);
			    result.addAll(aduContext.getAds());
			}
		}
		
		TxPduResponse resp = TxPduResponse.newBuilder()
				                 .setAdusListJson(result.toString())
				                 .build();
		responseObserver.onNext(resp);
		responseObserver.onCompleted();
	}
	
}
