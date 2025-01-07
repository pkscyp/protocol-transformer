

var LATLON_SCALE=Math.pow(10,7);

function gpstwap(pdu,aductx){

	if ( pdu.mth == 'sa') {
	  var sal = aductx.createGpsLocation(pdu.ts);
	  sal.source ='sa';
	  sal.latitude = Number(pdu.gnss.lat)/LATLON_SCALE;
	  sal.longitude = Number(pdu.gnss.lon)/LATLON_SCALE;
	  sal.altitude = pdu.gnss.alt;
	  sal.error = pdu.gnss.err;
	  sal.speed = pdu.gnss.mph;
	  sal.heading = pdu.gnss.hdg;
	  var locd = {
	    sat_count: pdu.gnss.cnt,
	    time_to_fix: pdu.gnss.ttf
	  }
	  sal.addinfo = JSLIB.toMap(locd);
	  aductx.addAdu(sal);
	}
	if ( pdu.mth != 'sa') {
	    var o = null;
	    if(pdu.accpt && pdu.ltetw){
	     	o= aductx.createWifiCellTowerLocation(pdu.ts);
	     	o.source="igw"
		    o.wifi_scans = pdu.accpt;
		    o.cell_towers = pdu.ltetw;    
		}
		
		if(pdu.accpt && !pdu.ltetw){
		   o = aductx.createWifiLocation(pdu.ts);
		   o.source="wip";
		   o.wifi_scans = pdu.accpt;
		   
		}
		if(!pdu.accpt && pdu.ltetw){
		   o = aductx.createCellTowerLocation(pdu.ts);
		   o.source="ctp";
		   o.cell_towers = pdu.ltetw;
		   
		}
	    if(o){
	      aductx.addAdu(o);
	    }
	}

    if(pdu.rqID > 0){
       var ar = aductx.createActionResponse(pdu.ts);
		  ar.request_id = pdu.rqID;
		  ar.status=0;
		  ar.source="gpstwap";
		  ar.type="location";
	     aductx.addAdu(ar);
    }
    var state = aductx.createAssetState(pdu.ts);
    state.source="gpstwap";
    state.battery_level=pdu.bat;
    state.under_temper=(pdu.sen>0)?1:0;
    if(pdu.accpt){
       state.wifi_count=pdu.accpt.size();
       var b=0;
       for( a in pdu.accpt ){
          if(a.beacon == true){
            ++b;
          }
       }
       state.paired_asset_count=b;
    }
    if(pdu.gnss && pdu.gnss.bcn){
       state.paired_asset_count=pdu.gnss.bcn;
    }
    if(pdu.ltetw){
       state.cell_tower_count=pdu.ltetw.size();
    }
    if(pdu.motion){
       state.motion = 1
    }
    state.tz_offset = pdu.loc;
    aductx.addAdu(state);
    
    if(pdu.sen > 0 ){
        var sto = aductx.createSensorTelemetry(pdu.ts);
	    var bitmask=OM500.NumberToBitIntArray(pdu.sen);
	    sto.source="gpstwap";
	    sto.sensor_id="gpstwap";
	    sto.snr_strap=bitmask[0];
	    sto.snr_backplate = bitmask[1];
	    sto.snr_ambient_light_tampr =  bitmask[2];
	    sto.snr_water = bitmask[3];
	    
	    aductx.addAdu(sto);
    }

    if( pdu.ltetw || pdu.accpt ){
       
       var conn=null;
       if(pdu.ltetw){
         var twp=pdu.ltetw.stream().filter(function(e){ return e.serving==true;}).findFirst();
         if(twp.isPresent()){
           conn = {type:'LTE',connected_to:twp.get().eucid};
         }        
       }
       
       if(conn==null && pdu.accpt){
         var twp = pdu.accpt.stream().filter(function(e) { return e.beacon==true;}).findFirst();
         if(twp.isPresent()){
           conn = {type:'WiFi',connected_to:twp.get().mac+"/"+twp.get().ssid};
         }
       }
     
       if(conn!=null){
         var c =  aductx.createConnectivity(pdu.ts);
         c.source="gpstwap";
         c.type=conn.type
         c.connected_to=conn.connected_to;
         aductx.addAdu(c);
         
       }
    }
}
