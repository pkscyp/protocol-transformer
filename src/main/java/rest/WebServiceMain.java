package rest;

import java.util.Map;

import org.rapidoid.RapidoidThing;
import org.rapidoid.http.Req;
import org.rapidoid.http.customize.HttpRequestBodyParser;
import org.rapidoid.job.Jobs;
import org.rapidoid.log.Log;
import org.rapidoid.setup.App;
import org.rapidoid.setup.On;

import u.JSTransformerHelper;

public class WebServiceMain  {

	private static final int port = 3000;
	
	public WebServiceMain() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		
		App.run(args);
		Log.options().fancy(false);
		JSTransformerHelper.with();
		On.custom().jsonRequestBodyParser(new DontParseJson());
		PduToAduTx handler = new PduToAduTx();
		App.beans(handler);
		
	}

	
  public static class DontParseJson extends RapidoidThing implements HttpRequestBodyParser {

	@Override
	public Map<String, ?> parseRequestBody(Req req, byte[] body) throws Exception {
		
		return null;
	}
	  
  }
	

}
