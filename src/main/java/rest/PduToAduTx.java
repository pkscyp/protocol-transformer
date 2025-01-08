package rest;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.pulsar.shade.com.google.gson.Gson;
import org.apache.pulsar.shade.com.google.gson.GsonBuilder;
import org.apache.pulsar.shade.com.google.gson.JsonObject;
import org.rapidoid.annotation.GET;
import org.rapidoid.annotation.Header;
import org.rapidoid.annotation.POST;
import org.rapidoid.http.Req;
import org.rapidoid.http.Resp;
import org.rapidoid.job.Jobs;
import org.rapidoid.log.Log;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.TypeReference;

import i.AduContext;
import i.GenericPdu;
import u.JSTransformerHelper;




public class PduToAduTx   {

	//final Gson gson = new GsonBuilder().setPrettyPrinting().create();
	
	public PduToAduTx() {
		// TODO Auto-generated constructor stub
	}

	@GET("/health/{name}")
	public void  health(Req req,Resp resp,String name) {
		
		 resp.code(200).json(Map.of("name",name,"System","Pankaj")).done();
	}
	
	
	@POST("/api/tx")
	public void transform(Req req,Resp resp,@Header("pulsarmsgid") String msgId) {
		
		Object js = JSON.parse(new String(req.body()));
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
				//aduContext.clearAds();
				JSTransformerHelper.with().invokeFunction(pdu_name, valueMap,aduContext);
				result.addAll(aduContext.getAds());
			}
		}
		//Log.info(result.toString());
		 resp.code(200).json(result).done();
	}

	
}
