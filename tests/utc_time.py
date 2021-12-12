
import datetime
import time
import pytz
import tzlocal

now = datetime.datetime.now()

utcnow = datetime.datetime.utcnow()

ltz = tzlocal.get_localzone()
# now2 = utcnow.replace(tzinfo=pytz.utc).astimezone(ltz)
now2 = utcnow.astimezone(ltz)
print(now,utcnow,now2)