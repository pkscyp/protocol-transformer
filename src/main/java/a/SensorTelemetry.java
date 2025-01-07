package a;

import java.util.Map;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;

import i.AbstractAdu;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class SensorTelemetry extends AbstractAdu {

	private static final long serialVersionUID = 1L;
	public String source;
	public String sensor_id;
	public Integer snr_backplate ;
	public Integer snr_strap;
	public Integer snr_ambient_light_tampr;
	public Integer snr_case;
	public Integer snr_water;
	public Integer snr_temperature;
	public Integer motion;
	public Map<String,Object> addinfo;
	
	public SensorTelemetry(String bundle_id,Integer its,String class_name) {
		super(bundle_id,its,class_name);
		
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "";
	}

}
