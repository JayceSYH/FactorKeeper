import datetime

def par_date(date_string):
    try:
        date = datetime.datetime.strptime(date_string, "%Y-%m-%d").date()
        return date
    except:
        return None