package i;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import a.*;


public class AduContext implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static ObjectMapper mapper = new ObjectMapper();
	final public String bid;
	final public String asset_type;
	final public String imei;
	public List<Map<String,Object>> ads;
	
	public AduContext(PduContext pctx) {
		this.bid=pctx.bid;
		this.imei = pctx.imei;
		this.asset_type=pctx.asset_type;
		this.ads = new LinkedList<Map<String,Object>>();
	}
	public AduContext(String bid) {
		this.bid=bid;
		this.imei=null;
		this.asset_type=null;
		this.ads = new LinkedList<Map<String,Object>>();
	}
	public void clearAds() {
		this.ads.clear();
	}
	public GpsLocation createGpsLocation(Integer ts) {
		GpsLocation l = new GpsLocation(bid,ts,"GpsLocation");
		return l;
	}
	public WifiLocation createWifiLocation(Integer ts) {
		return new WifiLocation(bid,ts,"WifiLocation");
	}
	
	public CellTowerLocation createCellTowerLocation(Integer ts) {
		return new CellTowerLocation(bid,ts,"CellTowerLocation");
	}
	
	public WifiCellTowerLocation createWifiCellTowerLocation(Integer ts) {
		return new WifiCellTowerLocation(bid,ts,"WifiCellTowerLocation");
	}
	public AssetState createAssetState(Integer ts) {
		return new AssetState(bid,ts,"State");
	}
	public AlertEvents createAlertEvents(Integer ts) {
		return new AlertEvents(bid,ts,"AlertEvents");
	}
	public SensorTelemetry createSensorTelemetry(Integer ts) {
		return new SensorTelemetry(bid,ts,"SensorTelemetry");
	}
	public AssetSettings createAssetSettings(Integer ts) {
		return new AssetSettings(bid,ts,"Settings");
	}
	public Connectivity createConnectivity(Integer ts) {
		return new Connectivity(bid,ts,"Connectivity");
	}
	public ActionResponse createActionResponse(Integer ts) {
		return new ActionResponse(bid,ts,"ActionResponse");
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
