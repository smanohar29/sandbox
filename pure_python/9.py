from datetime import datetime, date


print(str(datetime.now()).split(' ')[0])
print(str(date.today()))

currentDate = str(date.today())
test = "'" + currentDate + "'"


s = "SELECT loannumber, h_tgcid, leadqualityscore, lq_grade FROM leadallocation_conformed.lead_submissions WHERE receiveddate={}"
print(s.format(test))

t = str(datetime.now())
d = t.split('.')[0]
print(d)