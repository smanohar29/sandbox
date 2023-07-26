import json

loannumber = []

with open("data/Failed_SE_Messages.json", "r") as read_file:
    failedMessages = json.load(read_file)

for line in failedMessages:
    t = json.loads(line['result']['_raw'])
    loannumber.append(t['Properties']['Loan Number:'])

# print(loannumber)

print(len(loannumber))





