package f;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import i.AduContext;
import i.GenericPdu;
import i.PduContext;
import u.JSTransformerHelper;

public class ConvertToAdus extends RichMapFunction<PduContext,AduContext>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(ConvertToAdus.class);

	public ConvertToAdus() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public AduContext map(PduContext pctx) throws Exception {
		AduContext actx = new AduContext(pctx);
		for(GenericPdu gpdu: pctx.getPdus()) {
			try {
			JSTransformerHelper.with().invokeFunction(gpdu.name, gpdu.pdu,actx);
			}catch(Exception ex) {
				logger.error("TRANSERROR",ex);
			}
		}
		return actx;
	}

	
}
