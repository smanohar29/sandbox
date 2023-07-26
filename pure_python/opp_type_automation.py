oppTypes = {
       "types":
          ["retention refi with credit escalation",
          "retention refi with credit - va escalation",
          "retention refi with credit - banker action needed escalation",
          "retention refi with credit - credit score not displayed escalation",
          "retention refi apps escalation",
          "retention refi digital escalation",
          "retention refi credit trigger escalation",
          "retention refi 2nd voice with credit",
          "retention refi without credit reactive",
          "retention refi without credit proactive",
          "retention refi 2nd voice without credit",
          "retention missed appointment with credit",
          "retention missed appointment without credit",
          "purchase-serviced-escalation",
          "refinance-serviced-escalation",
          "refinance-serviced"]
}


searchText = 'refinance-serviced'

types = oppTypes['types']
# print("is element in: ")
# print(*types)

if searchText in types:
    print("element present")

query = '''
                SELECT * FROM t1 WHERE opptype in  

        '''

b = *types, sep=','

print(b)

