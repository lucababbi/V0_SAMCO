import calendar
from pandas.tseries.offsets import BDay

def get_datelist(startdate, enddate, months=[3,6,9,12]):
    """Return effective dates
    
    Keyword arguments:
    startdate -- earliest possible date (datetime.date(year, month, day))
    enddate -- last possible date (datetime.date(year, month, day))
    months -- review/rebalancing months ([int])
    implementation -- implementation days ((int,int))
        default: 3rd Friday (3,5)
    """
    datelist = []
    c = calendar.Calendar(firstweekday=calendar.SUNDAY)
    for year in range(startdate.year, enddate.year+1):
        for month in [x for x in range(1,13) if x in months]:
            monthcal = c.monthdatescalendar(year,month)
            third_friday = [day for week in monthcal for day in week if day.weekday() == calendar.FRIDAY and day.month == month][2]
            #implementationdate = calendar.Calendar(implementation[1]-1).monthdatescalendar(year, month)[implementation[0]][0]
            effectivedate = (third_friday + BDay(1)).date()
            datelist.append(effectivedate)
    return [x for x in datelist if x >= startdate if x <= enddate]