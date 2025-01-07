package i;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import pipeline.messages.BaseMessage;
import pipeline.messages.PduBundle;

public class PduBundleContext extends BaseMessage<PduBundleContext>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static ObjectMapper mapper = new ObjectMapper();

	private List<GenericPdu> pdus;
	

	public PduBundleContext(PduBundle record) {
		super();
		record.copyTo(this);
		this.pdus = new LinkedList<>();

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

	public PduBundleContext setPdus(List<GenericPdu> pdus) {
		this.pdus = pdus;
		return this;
	}
	

}
