package grpc;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Server;
import io.grpc.ServerBuilder;

public class TxPduServer {
	
	private static final Logger logger = LoggerFactory.getLogger(TxPduServer.class);
	

	public TxPduServer() {
		
	}

	public static void main(String[] args) throws Exception {
		logger.info("Starting the GRPC Server for TxPdu ...");
		 Server server = ServerBuilder.forPort(50051)
	                .addService(new TxPduServiceImpl())
	                .build();

	        server.start();
	        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
	        	logger.info("Received Shutdown Request");
	            server.shutdown();
	            logger.info("Successfully stopped the server");
	        }));

	        server.awaitTermination();

	}

}
