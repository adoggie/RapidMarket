
function encodeUtf8(text) {
    const code = encodeURIComponent(text);
    const bytes = [];
    for (var i = 0; i < code.length; i++) {
        const c = code.charAt(i);
        if (c === '%') {
            const hex = code.charAt(i + 1) + code.charAt(i + 2);
            const hexVal = parseInt(hex, 16);
            bytes.push(hexVal);
            i += 2;
        } else bytes.push(c.charCodeAt(0));
    }
    return bytes;
}

function decodeUtf8(bytes) {
    var encoded = "";
    for (var i = 0; i < bytes.length; i++) {
        encoded += '%' + bytes[i].toString(16);
    }
    return decodeURIComponent(encoded);
}

function ajax_post(url,method,data,success,error) {
     $.ajax({
                url:url,
                method: method,
                data:data,
                success:function (data,status,xhr) {
                    success(data)
                },
                error:function (xhr,status,error) {

                }
            });
}

// String.format = function() {
//     if( arguments.length == 0 )
//         return null;
//
//     var str = arguments[0];
//     for(var i=1;i<arguments.length;i++) {
//         var re = new RegExp('\\{' + (i-1) + '\\}','gm');
//         str = str.replace(re, arguments[i]);
//     }
//     return str;
// };


// '{0} {1}'.format('a','b');
String.prototype.format = function() {
    var args = arguments;
    return this.replace(/\{(\d+)\}/g,
        function(m,i){
            return args[i];
        });
};


function is_money(money) {
    var reg = /(^[1-9]([0-9]+)?(\.[0-9]{1,2})?$)|(^(0){1}$)|(^[0-9]\.[0-9]([0-9])?$)/;
    if (reg.test(money)) {
        return true;
    } else {
        return false;
    };
}

function is_num(value) {
    var reg = /^[0-9]*[1-9][0-9]*$/;
    if (reg.test(value)) {
        return true;
    } else {
        return false;
    };
}


/*将100000转为100,000.00形式*/
var dealNumber = function(money){
    if(money && money!=null){
        money = String(money);
        var left=money.split('.')[0],right=money.split('.')[1];
        right = right ? (right.length>=2 ? '.'+right.substr(0,2) : '.'+right+'0') : '.00';
        var temp = left.split('').reverse().join('').match(/(\d{1,3})/g);
        return (Number(money)<0?"-":"") + temp.join(',').split('').reverse().join('')+right;
    }else if(money===0){   //注意===在这里的使用，如果传入的money为0,if中会将其判定为boolean类型，故而要另外做===判断
        return '0.00';
    }else{
        return "";
    }
};

/*将100,000.00转为100000形式*/
var undoNubmer = function(money){
    if(money && money!=null){
        money = String(money);
        var group = money.split('.');
        var left = group[0].split(',').join('');
        return Number(left+"."+group[1]);
    }else{
        return "";
    }
};

// 新浪股票代码
function sina_code( code) {
    code = code.split('.')[0];
    if(code.substr(0,1) =='0' || code.substr(0,1) =='3' ){
        code = 'sz' + code;
    }else{
        code = 'sh' + code;
    }
    return code ;
}

//
function  num_fixed(value,precision) {
    return Number.parseFloat(x).toFixed(value,precision);
}