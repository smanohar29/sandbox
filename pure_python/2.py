# l = list(range(1,100))
#
# # lEven = filter(lambda f: True if(f%2 ==0) else False, l)
# lEven = filter(lambda f: f%2 ==0 , l)
#
# for i in lEven:
#     print(i)

# d1 = {1:'Hello', 2:'World'}
# d2 = {3:'How are you', 4:'Today'}
#
# print(d1)
# print(d2)
#
# d1.update(d2)
# print(d1)

# d={}
# for x in range(1,10):
#         d["label_group_{0}".format(x)]="Hello"
# print(d)

# splitValues = {}
# totalGroups = 4
#
# for i in range(1, totalGroups):
#         splitValues["split{0}".format(i)] = 0.0
#
# print(splitValues)

# new = list(splitValues)
# print(new)

# m = [1, 2, 3]
# for key in splitValues:
#         splitValues[value] =  int(splitValues[key]) + m[i]
#         i = i + 1
# print(splitValues)

# splitValues = {'split1': 0.0, 'split2': 0.0, 'split3': 0.0}
#
# dictlist = []
#
# for key, value in splitValues.items():
#     temp = [key,value]
#     dictlist.append(temp)
#
# print(dictlist)
# # print(len(dictlist))
#
# for i in range(0,len(dictlist)):
#         dictlist[i][1] = dictlist[i][1] + i
#
# print(dictlist)

max = 2147483647
min = -2147483648

splitValues = {'split1': 0.0, 'split2': 0.0, 'split3': 0.0}
splitValuesList = [['split1', 0.0], ['split2', 0.0], ['split3', 0.0]]

fractionPercent = ['0.75','0.10','0.10','0.5']

for i in range(0,len(splitValuesList)):
        if i==0:
                splitValuesList[i][1] = min + (max - min) * float(fractionPercent[i])

        if i>0:
                print("i:"+str(i))
                splitValuesList[i][1] = splitValuesList[i-1][1] + (max - min) * float(fractionPercent[i])


print(splitValuesList)