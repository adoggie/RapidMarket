#coding:utf-8
import datetime
import os , os.path
import gzip
# import StringIO

# def singleton(cls):
#     instance = cls()
#     instance.__call__ = lambda : instance
#     return instance


def singleton(cls):
    instances = {}

    def _singleton( *args, **kw):
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]
    return _singleton

class Singleton(object):
    @classmethod
    def instance(cls,*args,**kwargs):
        if not hasattr(cls,'handle'):
            cls.handle = cls(*args,**kwargs)
        return cls.handle

class ObjectCreateHelper(object):
    def __init__(self,func,*args,**kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def create(self):
        return self.func(*self.args,**self.kwargs)

class Instance(object):
    def __init__(self):
        self.handle = None
        self.helper = None

    def set(self,handle):
        self.handle = handle

    def __getattr__(self, item):
        return getattr(self.handle, item)

        # if self.handle:
        #     return getattr(self.handle,item)
        # if  self.helper:
        #     v = self.helper.create()
        #     return v


    def get(self):
        return self.handle


def hash_object(obj,key_prefix='',excludes=()):
    attrs = [s for  s in dir(obj) if not s.startswith('__')  ]
    kvs={}
    for k in attrs:
        attr = getattr(obj, k)
        if not callable(attr):
            if k in excludes:
                continue
            if key_prefix:
                k = key_prefix + k
            kvs[k] = attr


    return kvs

def object_assign(obj,values,add_new=False):
    """将values的key对应的value赋给对象obj的属性"""
    attrs = [s for  s in dir(obj) if not s.startswith('__')  ]
    kvs={}
    for k,v in values.items():
        attr = getattr(obj, k,None)
        if attr !=None:
            # if k =='id':
            #     print k
            is_property = False
            if hasattr(obj.__class__,k):
                if isinstance(getattr(obj.__class__, k), property):
                    is_property = True
            if  callable(attr) or is_property:
               continue # 函数,属性不能被自动替换
        # print 'k:',k
        if not add_new:
            if k in attrs:
                setattr(obj,k,v)
        else:
            setattr(obj,k,v)

def get_config_item(root,path,default=None):
    """根据配置路径 x.y.z ,获取配置项"""
    ss = path.split('.')
    conf = root
    try:
        for s in ss:
            conf = conf.get(s)
            if not conf:
                break
    except:
        conf = default
    return conf

def list_item_match(list_,name,value):
    """return first item which matched"""
    matched = filter(lambda x:x.get(name)==value,list_)
    if matched:
        return matched[0]
    return None


class ObjectBuilder(object):
    @staticmethod
    def create(data):
        """
        从 dict 数据生成 object 对象
        :param data:dict
        :return:
        """
        class _Object:pass
        if not isinstance(data,dict):
            return data
        newobj = _Object()
        for k,v in data.items():
            setattr( newobj,str(k),v)
        return newobj

def string_list(s,sep=','):
    return map(str.strip,s.split(sep))

class Sequence(object):
    def __init__(self,init_val = 0,step=1):
        self.value = init_val
        self.step = step

    def next(self):
        self.value+=self.step
        return self.value


def cleaned_json_data(rs,excludes=['_id']):
    """清除非json格式化的字段"""
    for r in rs:
        keys = []
        for k,v in r:
            if not isinstance(v,(str,unicode,float,int,bool)):
                keys.append(k)
        for ex in excludes:
            keys.append(ex)

        for k in keys:
            if r.has_key(k):
                del r[k]
    return rs


def make_hash(password,key,salt=None):
    import hashlib
    if not salt :
        salt = make_salt()
    password = hashlib.md5(password+salt+key).hexdigest()
    return password,salt

def encrypt_text(text,secret):
    """加密文本"""
    return text

def decrypt_text(text,secret):
    """解密文本"""
    return text

def make_password(num=6,chars='0123456789'):
    import random
    password = ''.join(map(lambda _: str(_), [ chars[random.randint(0, len(chars))] for _ in range(num)]))
    return password

def make_salt(random=''):
    import uuid
    return uuid.uuid4().hex


def hex_dump( bytes):
    dump = ' '.join(map(lambda _: '%02x' % _, map(ord, bytes)))
    return dump





def moneyfmt(value, places=2, curr='', sep=',', dp='.', pos='', neg='-', trailneg=''):
    """Convert Decimal to a money formatted string.

    places:  required number of places after the decimal point
    curr:    optional currency symbol before the sign (may be blank)
    sep:     optional grouping separator (comma, period, space, or blank)
    dp:      decimal point indicator (comma or period)
             only specify as blank when places is zero
    pos:     optional sign for positive numbers: '+', space or blank
    neg:     optional sign for negative numbers: '-', '(', space or blank
    trailneg:optional trailing minus indicator:  '-', ')', space or blank

    >>> d = Decimal('-1234567.8901')
    >>> moneyfmt(d, curr='$')
    '-$1,234,567.89'
    >>> moneyfmt(d, places=0, sep='.', dp='', neg='', trailneg='-')
    '1.234.568-'
    >>> moneyfmt(d, curr='$', neg='(', trailneg=')')
    '($1,234,567.89)'
    >>> moneyfmt(Decimal(123456789), sep=' ')
    '123 456 789.00'
    >>> moneyfmt(Decimal('-0.02'), neg='<', trailneg='>')
    '<0.02>'

    """
    from decimal import Decimal
    if not value:
        value = 0
    value = Decimal(value)
    q = Decimal(10) ** -places  # 2 places --> '0.01'
    sign, digits, exp = value.quantize(q).as_tuple()
    result = []
    digits = map(str, digits)
    build, next = result.append, digits.pop
    if sign:
        build(trailneg)
    for i in range(places):
        build(next() if digits else '0')
    build(dp)
    if not digits:
        build('0')
    i = 0
    while digits:
        build(next())
        i += 1
        if i == 3 and digits:
            i = 0
            build(sep)
    build(curr)
    build(neg if sign else pos)
    return ''.join(reversed(result))


# print moneyfmt('1234.45', sep=',', dp='.')

def normal_number(x,default=0,type=int):
    value = default
    try:
        value = type(x)
    except: pass
    return value


def make_dir(*args):
  path = os.path.join(*args)
  if not os.path.exists(path):
    os.makedirs(path)

def open_file(filename,mode='w'):
  dirname = os.path.dirname( filename )
  make_dir(dirname)
  return open(filename,mode)



def gzip_compress(buf):
    out = StringIO.StringIO()
    with gzip.GzipFile(fileobj=out, mode="w") as f:
        f.write(buf)
    return out.getvalue()


def gzip_decompress(buf):
    obj = StringIO.StringIO(buf)
    with gzip.GzipFile(fileobj=obj) as f:
        result = f.read()
    return result

def input_params(line, prefix, least=0 ,help='error: invalid parameter.'):
  """ 得到命令行输入参数
  :param pattern:  [a,b]
  :param least :  最少右侧参数个数
  :return: 如果无法匹配，返回 None 或 []

    args = input_params("show ball 100 2 3 4",["show","ball"],1)
    if not args:
      print 'need more param!'
    print args   # ['100', '2', '3', '4']
  """
  fs = line.split()
  if len(prefix) > len(fs):
    # print help
    return []

  # print tuple(prefix) , fs[:len(prefix)]

  if tuple(prefix) != tuple(fs[:len(prefix)]):
    # print help
    return []
  if least == 0:
    return fs

  right = fs[len(prefix):]
  if len(right) < least:
    # print help
    return []
  return right

def utc_timestap(dt = None):
    if not dt:
        dt = datetime.datetime.now()
    dt = dt.utcnow()
    ts = int( dt.timestamp() * 1000 )
    return ts