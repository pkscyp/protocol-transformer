package i;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class PduContext {

	private static ObjectMapper mapper = new ObjectMapper();
	final public String bid;
	public String asset_type;
	public String imei;
	private List<GenericPdu> pdus;
	

	public PduContext(String asset_type, String imei) {
		super();
		this.asset_type = asset_type;
		this.imei = imei;
		this.pdus = new LinkedList<>();
		this.bid = UUID.randomUUID().toString();
	}
	
	public void addPdu(GenericPdu gp) {
		this.pdus.add(gp);
	}
	
	public void sortPdusByTime() {
		Collections.sort(pdus,(a,b) -> a.getTimeOfPdu().compareTo(b.getTimeOfPdu()));
	}
	
	public String toString() {
		try {
			return mapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "ERROR";
	}

	public List<GenericPdu> getPdus() {
		return pdus;
	}

	public void setPdus(List<GenericPdu> pdus) {
		this.pdus = pdus;
	}
	

}
