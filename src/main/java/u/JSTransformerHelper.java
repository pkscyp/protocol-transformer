package u;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;

import org.openjdk.nashorn.api.scripting.NashornScriptEngine;
import org.openjdk.nashorn.api.scripting.ScriptObjectMirror;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JSTransformerHelper {

	private static final Logger logger = LoggerFactory.getLogger(JSTransformerHelper.class);
	
	final ScriptEngineManager scriptEngineManager;
	public ThreadLocal<Bindings> bindingsQ = null;

	private NashornScriptEngine  engine;
	
	private Properties config;
	private CompiledScript  _comscript;
	
	private static final OnDemandInit<JSTransformerHelper> DEFAULT = new OnDemandInit<JSTransformerHelper>(new Callable<JSTransformerHelper>() {

		@Override
		public JSTransformerHelper call() throws Exception {
			JSTransformerHelper inst = new JSTransformerHelper();
			inst.init();
			
			return inst;
		}
		
	});
	
	public static JSTransformerHelper with() {
		return DEFAULT.get();
	}
	
	private JSTransformerHelper() {
		scriptEngineManager = new ScriptEngineManager();
	}

	private void init() {
		
		createScriptingEngine();
		loadAllscript();
		bindingsQ = new ThreadLocal<Bindings>() {
			@Override
	        protected Bindings initialValue()
	        {
				logger.info("Creating new Bindings");
	            return engine.createBindings();
	        }
		};
	}
	
	

	private void loadAllscript() {
		File folder = new File("jsscripts");
		if(!folder.exists()||!folder.isDirectory()) {
			throw new RuntimeException("jsscripts folder not found");
		}

		
		try {

			
			File s = new File(folder,"om500_trns.js");
			if(!s.exists())
				throw new Exception("Script om500_trns.js Not Found");
			String content = Files.readString(s.toPath(), StandardCharsets.UTF_8);
			this.engine.eval(content);
			_comscript = this.engine.compile("var __compile_js_dummy=true;");
			
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		} 
		
		
	}
	
	public void invokeFunction(String fname,Object ... args) {
		Bindings b = bindingsQ.get();
		b.clear();
		try {
			_comscript.eval(b);
		} catch (ScriptException e) {
			logger.error("binadings",e);
			return;
		}
		ScriptObjectMirror fn = (ScriptObjectMirror)b.get(fname);
		if(fn!=null && fn.isFunction()) {
			fn.call(null, args);
		}else {
			logger.info("Function Not Found {} ",fname);
		}
		
	}
	
	public Optional<String> standardizePdu(String fname, Object ...args) {
		Bindings b = bindingsQ.get();
		b.clear();
		try {
			_comscript.eval(b);
		} catch (ScriptException e) {
			logger.error("binadings",e);
			return Optional.empty();
		}
		ScriptObjectMirror fn = (ScriptObjectMirror)b.get(fname);
		if(fn!=null && fn.isFunction()) {
		   return	Optional.of((String) fn.call(null, args));
		}else {
			logger.info("Function Not Found {} ",fname);
		}
		return Optional.empty();
	}

	private void createScriptingEngine() {
		this.engine = (NashornScriptEngine) scriptEngineManager.getEngineByName("nashorn");
		if(this.engine==null) {
			throw new RuntimeException("Scripting Enine not found");
		}
		this.engine.put("JsonUtils", JsonUtils.with());
		this.engine.put("logger", logger);
	}
	
	public void listAvailableScriptEngine() {
		try {
		
		 List<ScriptEngineFactory> scriptEngineFactories = scriptEngineManager.getEngineFactories();
		 logger.info(" Found {} ",scriptEngineFactories.size());
		 if (scriptEngineFactories != null) {
			 for (ScriptEngineFactory factory : scriptEngineFactories) {
				 if (factory.getScriptEngine() instanceof Invocable) {
					 logger.info(" Available Engine is {}",factory.getNames());
				 }
			 }
		 }
		}catch(Exception ex) {
			logger.error("Scriting Manager", ex);
		}
	}

	public Optional<Map<String, Object>> convertPdu(String fname, Object ... args ) {
		Bindings b = bindingsQ.get();
		b.clear();
		try {
			_comscript.eval(b);
		} catch (ScriptException e) {
			logger.error("bindings",e);
			return Optional.empty();
		}
		try {
			ScriptObjectMirror fn = (ScriptObjectMirror)b.get(fname);
			if(fn!=null && fn.isFunction()) {
			   return	Optional.of((Map<String, Object>) fn.call(null, args));
			}else {
				logger.info("Function Not Found {} ",fname);
			}
		}catch(Exception ex) {
			logger.error("convertPdu Error",ex);
		}
		return Optional.empty();
	}
}
