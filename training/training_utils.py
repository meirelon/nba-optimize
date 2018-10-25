from dateutil import rrule, parser

def generate_date_list(date1, date2):
    date1 = '2017-10-17'
    date2 = '2018-04-11'

    return [x.strftime("%Y-%m-%d") for x in list(rrule.rrule(rrule.DAILY,
                             dtstart=parser.parse(date1),
                             until=parser.parse(date2)))]
