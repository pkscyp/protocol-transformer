var sys_JSON=JSON;

var JSON={
    stringify:function (input){
       var input_type = Object.prototype.toString.call(input);
       if(input_type === "[object Object]" ||
          input_type === "[object Array]" ){
          return sys_JSON.stringify(input);
       }else {
          print('using JsonUtils');
          return JsonUtils.stringify(input);
       }
    },
    
    parse:function(input){
      return sys_JSON.parse(input);
    }
}

var JSLIB = {
   toMap: function(jsobj){
		  var m = new Map();
		  for(i in jsobj){
		   if(jsobj[i]){  m.put(i,jsobj[i]);}
		  }
		  return m;
		}
}