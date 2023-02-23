def schema_from_list(columnlist, outfilename, meta_cols = True):
    schemablock = """
        {
            "name": "FIELDNAME",
            "type": "FIELDTYPE"
        }"""
    fieldtype = {
        1: "STRING",
        2: "NUMERIC",
        3: "INT64",
        4: "DATETIME",
        5: "DATE",
        6: "TIMESTAMP",
        7: "BOOLEAN",
        8: "CUSTOM"
    }
    with open(outfilename, 'w') as writer:
        writer.write("[")
        for col in columnlist:
            print(f"{col}: 1 - STRING, 2 - NUMERIC, 3 - INT64, 4 - DATETIME, 5 - DATE, 6 - TIMESTAMP, 7 - BOOLEAN, 8 - custom")
            index = input()
            print(index)
            select = fieldtype.get(int(index), "CUSTOM")
            if select == "CUSTOM":
                print("Enter field type:")
                select = input()

            writer.write(schemablock.replace("FIELDNAME", col).replace("FIELDTYPE", select))
            if meta_cols or col != columnlist[-1]:
                writer.write(",")
        if meta_cols:
            writer.write(schemablock.replace("FIELDNAME","Inserted_Timestamp").replace("FIELDTYPE", "DATETIME"))
            writer.write(",")
            writer.write(schemablock.replace("FIELDNAME","Filename").replace("FIELDTYPE", "STRING"))
        writer.write(']')