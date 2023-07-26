count = 0

# function to generate final table struture
def generateFields(count):
    sample = """            "Name": "post_prop{count}",
            "Type": "string" """

    sample = sample.format(count=count)
    print("            {")
    print(sample)
    print("            },\n")



with open("data/adobe_dq_prcd_data_1.txt", "r") as file:
    for line in file:
        if "Name" in line:
            pos1 = line.find('"Name": "')
            pos2 = line.find('",')

            field = line[pos1+9 : pos2]

            count = count + 1
            generateFields(count)




