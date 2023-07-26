# function to generate final table struture
def generateFields(line):
    line = line.strip()
    fieldName = line.split(' ')[0]
    fieldType = line.split(' ')[1]

    sample = """            "Name": "{name}",
            "Type": "{type}" """

    sample = sample.format(name=fieldName, type=fieldType)
    print("            {")
    print(sample)
    print("            },\n")



with open("data/adobe_dq_prcd_data_2.txt", "r") as file:
    for line in file:
        generateFields(line)
