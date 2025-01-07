package i;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import a.*;
import pipeline.messages.BaseMessage;


public class AduBundleContext extends BaseMessage<AduBundleContext> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static ObjectMapper mapper = new ObjectMapper();
	public List<Map<String,Object>> ads;
	
	public AduBundleContext(PduBundleContext pctx) {		
		this.ads = new LinkedList<Map<String,Object>>();
		pctx.copyTo(this);
	}

	public GpsLocation createGpsLocation(Integer ts) {
		GpsLocation l = new GpsLocation(getPulsarMessageId(),ts,"GpsLocation");
		return l;
	}
	public WifiLocation createWifiLocation(Integer ts) {
		return new WifiLocation(getPulsarMessageId(),ts,"WifiLocation");
	}
	
	public CellTowerLocation createCellTowerLocation(Integer ts) {
		return new CellTowerLocation(getPulsarMessageId(),ts,"CellTowerLocation");
	}
	
	public WifiCellTowerLocation createWifiCellTowerLocation(Integer ts) {
		return new WifiCellTowerLocation(getPulsarMessageId(),ts,"WifiCellTowerLocation");
	}
	public AssetState createAssetState(Integer ts) {
		return new AssetState(getPulsarMessageId(),ts,"State");
	}
	public AlertEvents createAlertEvents(Integer ts) {
		return new AlertEvents(getPulsarMessageId(),ts,"AlertEvents");
	}
	public SensorTelemetry createSensorTelemetry(Integer ts) {
		return new SensorTelemetry(getPulsarMessageId(),ts,"SensorTelemetry");
	}
	public AssetSettings createAssetSettings(Integer ts) {
		return new AssetSettings(getPulsarMessageId(),ts,"Settings");
	}
	public Connectivity createConnectivity(Integer ts) {
		return new Connectivity(getPulsarMessageId(),ts,"Connectivity");
	}
	public ActionResponse createActionResponse(Integer ts) {
		return new ActionResponse(getPulsarMessageId(),ts,"ActionResponse");
	}
	
	public void addAdu(AbstractAdu adu) {
		Map<String,Object> m = new HashMap<>();
		m.put(adu.getClassName(),adu);
		ads.add(m);
	}
	
	public String toString() {
		try {
			return mapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			
		}
		return "ErrorJSON";
	}

	public List<Map<String, Object>> getAds() {
		return ads;
	}

	public void setAds(List<Map<String, Object>> ads) {
		this.ads = ads;
	}
	
}
