import calendar

def get_first_and_third_tuesdays(year, month):
    c = calendar.Calendar()
    tuesdays = [day for day in c.itermonthdates(year, month)
                if day.weekday() == 1 and day.month == month]
    return tuesdays[0], tuesdays[2]
