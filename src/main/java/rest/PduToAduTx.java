package rest;

import java.util.List;
import java.util.Map;

import org.apache.pulsar.shade.com.google.gson.Gson;
import org.apache.pulsar.shade.com.google.gson.GsonBuilder;
import org.apache.pulsar.shade.com.google.gson.JsonObject;
import org.rapidoid.annotation.GET;
import org.rapidoid.annotation.POST;
import org.rapidoid.http.Req;
import org.rapidoid.http.Resp;
import org.rapidoid.job.Jobs;
import org.rapidoid.log.Log;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;




public class PduToAduTx   {

	final Gson gson = new GsonBuilder().setPrettyPrinting().create();
	
	public PduToAduTx() {
		// TODO Auto-generated constructor stub
	}

	@GET("/health/{name}")
	public void  health(Req req,Resp resp,String name) {
		
		 resp.code(200).json(Map.of("name",name,"System","Pankaj")).done();
	}
	
	
	@POST("/api/tx")
	public void transform(Req req,Resp resp) {
		Map js = JSON.parseObject(new String(req.body()),new TypeReference<Map>() {});
		
		Log.info(js.toString());
		 resp.code(200).json(js).done();
	}

	
}
