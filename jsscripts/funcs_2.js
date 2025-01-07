

function ev_toMap(pdulist){
  var pl = JSON.parse(pdulist);
  var map= new Map();
    map.ts = pl[0];
    map.loc = pl[1];
    map.code = pl[2];
    map.val1 = pl[3]?pl[3]:null;
    map.val2 = pl[4]?pl[4]:null;
    
    return map;
}
function bcnEv_toMap(pdulist){
  var pl = JSON.parse(pdulist);
  var map= new Map();
    map.ts = pl[0];
    map.loc = pl[1];
    map.code = pl[2];
    map.bcnid = pl[3]?pl[3]:null;
    map.val1 = pl[4]?pl[4]:null;
    
    return map;
}



function stmsg(pdu,ctx){
   var state = ctx.createAssetState(pdu.ts);
   state.source="stmsg";
   state.battery_level=pdu.bl;
   state.under_temper=pdu.tmpr;
   state.paired_asset_count=pdu.bcn;
   state.wifi_count=pdu.wir;
   state.cell_tower_count=pdu.cir;
   var sdata = {
      snsr: pdu.snsr,
      wifi_scan_tm: pdu.wsts,
      xtra:pdu.xtra
   }
   state.addinfo = JSLIB.toMap(sdata);
   ctx.addAdu(state);
   var cfg = ctx.createAssetSettings(pdu.ts);
   cfg.name="GeoFence";
   cfg.addinfo = JSLIB.toMap({fence_count:pdu.gfn,fence_hash:pdu.gfh,fence_crossed:pdu.gfx});
   ctx.addAdu(cfg);
   
   if(pdu.tmpr > 0){
        var sto = ctx.createSensorTelemetry(pdu.ts);
	    var bitmask=OM500.NumberToBitIntArray(pdu.tmpr);
	    sto.source="stmsg";
	    sto.sensor_id="stmsg";
	    sto.snr_strap=bitmask[0];
	    sto.snr_backplate = bitmask[1];
	    sto.snr_ambient_light_tampr =  bitmask[2];
	    sto.snr_water = bitmask[3];
	    ctx.addAdu(sto);
   }
}


function ack(pdu,ctx){
  var ar = ctx.createActionResponse(pdu.ts);
  ar.request_id = pdu.rqID;
  ar.status=pdu.resp;
  ar.source="ack";
  ar.type="ack";
  ctx.addAdu(ar);
}


function bcnEv(pdu,ctx){
  print(' bcnEnv Not Implemented ',JSON.stringify(pdu));
}



function ev(pdu,ctx){
  
  var ae = ctx.createAlertEvents(pdu.ts);
  ae.source="ev";
  ae.code = pdu.code
  ae.addinfo = JSLIB.toMap({val1: pdu.val1?pdu.val1:null,val2:pdu.val2?pdu.val2:null});
  ctx.addAdu(ae);
  var state = ctx.createAssetState(pdu.ts);
	state.source="ev";
	state.tz_offset =  pdu.loc;

  if(OM500.TMPR_YES_CODE.indexOf(pdu.code)>=0){
    state.under_temper=1;
  }
  if(OM500.TMPR_NO_CODE.indexOf(pdu.code)>=0){
    state.under_temper=0;
  }
  if(OM500.BAT_VAL_CODES.indexOf(pdu.code) >=0 ){
        
	 state.on_charger = pdu.val2?pdu.val2:-1;
	 state.battery_level = pdu.val1?pdu.val1:-1;

	  
  }
  ctx.addAdu(state);
  
  
  if(OM500.TMPR_CODES.indexOf(pdu.code)>=0){
    var st = ctx.createSensorTelemetry(pdu.ts);
    st.source="ev";
    if(pdu.code == 'Str' || pdu.code == 'StrC'){
       st.sensor_id="StrapSensor";
       st.snr_strap= (pdu.code == 'Str')? 1: 0;
       st.addinfo = JSLIB.toMap({ledOnLightLevel:pdu.val1,ledOffLightLevel:pdu.val2});
    }
    if(pdu.code == 'BkP' || pdu.code == 'BkPC'){
       st.sensor_id="BackplateSensor";
       st.snr_backplate= (pdu.code == 'BkP')? 1: 0;
    }
    if(pdu.code == 'Wtr' || pdu.code == 'WtrC'){
       st.sensor_id="WaterSensor";
       st.snr_water= (pdu.code == 'Wtr')? 1: 0;
    }
    if(pdu.code == 'Case' || pdu.code == 'CaseC'){
       st.sensor_id="CaseSensor";
       st.snr_case= (pdu.code == 'Case')? 1: 0;
    }
    ctx.addAdu(st);
  }
}