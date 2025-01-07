package f;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import i.AduBundleContext;
import i.AduContext;
import i.GenericPdu;
import i.PduBundleContext;
import i.PduContext;
import u.JSTransformerHelper;

public class FnPduToAduContext extends RichMapFunction<PduBundleContext,AduBundleContext>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(FnPduToAduContext.class);

	public FnPduToAduContext() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public AduBundleContext map(PduBundleContext pctx) throws Exception {
		AduBundleContext actx = new AduBundleContext(pctx);
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
