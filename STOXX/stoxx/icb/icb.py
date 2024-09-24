import csv
import datetime
import pandas
class ICB():

    def __init__(self):
        self.data={}
        with open("S:\Stoxx\Product Development and Research\Python\data\icb\icb_americas.csv", 'r') as csvfile:
            headers = {}
            spamreader = csv.reader(csvfile, delimiter=';')
            headersLine = next(spamreader)

            i = 0
            for cell in headersLine:
                headers[cell] = i
                i += 1

            for row in spamreader:

                if row[headers["SEDOL"]] not in self.data.keys():
                    self.data[row[headers["SEDOL"]]]=[[row[headers["SEDOL"]],datetime.datetime.strptime(row[headers["date"]],"%d/%m/%Y").date(),row[headers["new ICB"]]]]

                else:
                    if row[headers["new ICB"]]!='death':
                        temp=self.data[row[headers["SEDOL"]]]
                        temp.append([row[headers["SEDOL"]],datetime.datetime.strptime(row[headers["date"]],"%d/%m/%Y").date(),row[headers["new ICB"]]])

    def getICB(self,SEDOL,date):
        try:
            datePy=date.to_pydatetime().date()
        except:
            datePy=date
        temp= self.data.get(SEDOL,[])
        tempICB=""
        if len(temp)==0:
            return tempICB
        else:
            tempICB=sorted(temp,key=lambda tup: tup[1])[0][2]
            for line in sorted(temp,key=lambda tup: tup[1]):
                if line[1]< datePy:
                    tempICB=line[2]
            return tempICB
#icb=ICB()
#print(icb.getICB("B28J1X2",datetime.date(2010, 2, 12)))
