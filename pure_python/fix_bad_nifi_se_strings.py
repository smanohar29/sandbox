import json

validOutput = open('data/nifi_bad_strings/output.json', 'a')

# function to parse out all the necessary fields needed from submission string
def calculateFields(data):
    fieldList = []

    payload = data[ data.find('Payload')+ 9 : ]
    payload = payload.split(',')

    for i in payload:
        fieldList.append(i.split(':')[0])

    return fieldList


# function to fix JSON formatting
def fixBadNifiString(data):
    result = ''

    part1 = data[ : data.find('"Payload')]
    part2 = data[data.find('"Payload') : ]

    payload = part2.replace('"', '')
    fields = calculateFields(payload)

    for i in range(0,len(fields)):
          if fields[i] in payload:
                if i == len(fields) - 1:
                    pos1 = payload.find(fields[i]) + len(fields[i]) + 1
                    pos2 = payload.find('}}') + 1

                else:
                    pos1 = payload.find(fields[i]) + len(fields[i]) + 1
                    pos2 = payload.find(fields[i + 1])

                value = payload[pos1:pos2-1]
                payload = payload[pos2:]

                if value not in ('null', 'false', 'true'):
                    value = '"' + value + '"'

                result = result + '"' + fields[i] + '":' + value + ','

    fixedJson = part1 + '"Payload":{' + result
    fixedJson1 = fixedJson[:-1]
    fixedJson2 = fixedJson1 + '}}'

    validOutput.write(fixedJson2)
    validOutput.write('\n')


with open("data/nifi_bad_strings/data1.txt", "r") as file:
    for line in file:
        fixBadNifiString(line)



