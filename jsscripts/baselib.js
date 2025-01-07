var Map = Java.type("java.util.LinkedHashMap");
var List = Java.type("java.util.LinkedList");

var OM500={
  
  StringLpad: function(s,len,cstr){
      while (s.length < len) s = cstr + s; 
   	  return s;
  },
  NumberToBitIntArray: function(n) {
       s = Number(n).toString(2);
       s = this.StringLpad(s,20,"0");
       return s.split('').reverse().map(function(s) { return parseInt(s); });
  },
  
  BAT_VAL_CODES:['OnChg','OffChg','BChg','EChg','BtF','BtU','BtW','BtL','BtC'],

  TMPR_YES_CODE:['Str','BkP','Case','Wtr'],
  TMPR_NO_CODE:['StrC','BkPC','CaseC','WtrC'],
  TMPR_CODES:['Str','BkP','Case','Wtr','StrC','BkPC','CaseC','WtrC']
  
}


