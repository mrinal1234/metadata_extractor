import os
import glob
import re
import time
import numpy as np
import pandas as pd

pd.set_option("display.max_rows", 1000)
pd.set_option("display.max_colwidth", 2000)

os.chdir("/workspace/pepsico/")
ROOT_DIR = os.path.abspath("")
INPUT_DIR = os.path.join(ROOT_DIR, "input")
OUTPUT_DIR = os.path.join(ROOT_DIR, "output")
Procedure = ""

REG_BLOCK_COMMENT = re.compile("(/\*)[\w\W]*?(\*/)", re.IGNORECASE)
REG_LINE_COMMENT = re.compile("(--.*)", re.IGNORECASE)


def split_table_info(table_info: str):
    """
    Split schema.table into schema and table
    """
    table_info = table_info.split(".")
    # print('table_info', table_info)
    if len(table_info) >= 3:
        table_name = table_info[-1]
        table_schema_name = table_info[-2]
    elif len(table_info) == 2:
        table_name = table_info[1]
        table_schema_name = table_info[0]
    else:
        table_name = table_info[0]
        table_schema_name = ""

    return table_schema_name, table_name


def split_procedure_to_commits(sqlCommands: list):
    """
    Split code script procedures into commits
    """
    stored_procedure = []
    for procedure in sqlCommands:
        if re.search("COMMIT;", procedure):
            commit = procedure.strip("\n").split("COMMIT;")
            commit = [com.strip(" ") for com in commit if com != ""]
            # print('commit', commit)
            commit_no = len(commit)
            stored_procedure.append([commit_no, procedure, commit])
        else:
            commit = [procedure]
            commit_no = 1
            stored_procedure.append([commit_no, procedure, commit])

    return stored_procedure


def schema_procedure_name(block: str):
    """
    Extract schema and view name from SQL query
    """
    global xref_ProcName
    metadata = []
    for s in block.split("\n"):
        if re.search("^PROCEDURE", s.strip()):
            schema_proc = s.strip().split("PROCEDURE")[-1].strip()
            # print('schema_proc', schema_proc)
            Schema, xref_ProcName = split_table_info(schema_proc)
            metadata.append([Schema, xref_ProcName])
        else:
            pass

    return xref_ProcName, metadata


def procedure_commit_parsing(commit: str) -> list:
    """
    Parse each commit to extract required metadata
    """
    metadata = []
    for s in commit.split("\n"):
        # Line Comments
        if re.search("(--.*)", s):
            s = s.replace(s, "")
        if re.search("\s*JOIN\s+$", s):
            pass
        if re.search("\s*JOIN\n$", s):
            pass

        # insert into
        if re.search("\s*INSERT INTO\s", s):
            Operation = "Insert into"
            table_info = (
                s.strip().split("INSERT INTO")[-1].split()[0]
            )  # Split 'INSERT INTO'
            # print('insert into table:', table_info)
            Table_Schema_Name, Table = split_table_info(table_info)
            metadata.append([Operation, Table_Schema_Name, Table])

        # update
        if re.search("\s*UPDATE\s", s):
            Operation = "Update"
            table_info = s.strip().split(" ")[1]  # split 'UPDATE'
            # print('update table info:', table_info)
            Table_Schema_Name, Table = split_table_info(table_info)
            metadata.append([Operation, Table_Schema_Name, Table])

        # delete from + select from
        if re.search("^(?=.*(?:DELETE FROM))(?=.*(?:SELECT FROM))", s):
            Operation = "Delete from"
            table_info1 = (
                s.strip()
                .split("DELETE FROM")[-1]
                .split()[0]
                .replace(")", " ")
                .split()[0]
            )
            Table_Schema_Name, Table = split_table_info(table_info1)
            metadata.append([Operation, Table_Schema_Name, Table])
            Operation = "Select from"
            table_info2 = (
                s.strip().split("FROM")[-1].split()[0].replace(")", " ").split()[0]
            )
            Table_Schema_Name, Table = split_table_info(table_info2)
            metadata.append([Operation, Table_Schema_Name, Table])

        # only delete from
        if re.search("(\s*DELETE FROM\s)(?!.*(?:FROM.*))", s):
            Operation = "Delete from"
            table_info = s.strip().split(" ")[2]  # split 'DELETE FROM'
            # print('delete from table:', table_info)
            Table_Schema_Name, Table = split_table_info(table_info)
            metadata.append([Operation, Table_Schema_Name, Table])

        # merge into
        if re.search("\s*MERGE INTO\s", s):
            Operation = "Merge into"
            table_info = s.strip().split(" ")[2]  # split 'MERGE'
            # print('merge into table:', table_info)
            Table_Schema_Name, Table = split_table_info(table_info)
            metadata.append([Operation, Table_Schema_Name, Table])

        # cursor
        if re.search("\s*CURSOR\s", s):
            Operation = "Cursor"
            table_info = s.strip().split(" ")[1]  # split 'CURSOR'
            Table_Schema_Name, Table = split_table_info(table_info)
            metadata.append([Operation, Table_Schema_Name, Table])

        # truncate table
        if re.search("\s*TRUNCATE\s+TABLE\s+", s):
            Operation = "Truncate table"
            table_info = (
                s.strip().split("TRUNCATE TABLE")[-1].replace("'", " ").split()[0]
            )  # split 'CURSOR'
            Table_Schema_Name, Table = split_table_info(table_info)
            metadata.append([Operation, Table_Schema_Name, Table])

        # from
        if re.search("^(?!.*(?:DELETE))(?=\s*(?:\w+FROM_\w+))", s):
            pass

        if re.search("^(?!.*(?:DELETE))(?=.*(?:FROM)\s+(\w+))", s):
            # print('\n----------------------')
            # print('s:',s)
            Operation = "Select from"
            table_info = (
                s.strip().split("FROM")[-1].split()[0].replace(")", " ").split()[0]
            )
            # print(f'select from :{table_info}')
            Table_Schema_Name, Table = split_table_info(table_info)
            # print(f'Table_Schema_Name:{Table_Schema_Name}\n Table:{Table}')
            # print('\n----------------------\n')
            metadata.append([Operation, Table_Schema_Name, Table])

        if re.search("^(?!.*(?:DELETE))(?=.*(?:FROM$))", s):
            pass

        if re.search("\s*JOIN\s+$", s):
            pass
        if re.search("\s*JOIN\n$", s):
            pass
        if re.search("\s.JOIN\s+\(SELECT\s+(\w+).*FROM\s+$", s):
            pass

        if re.search("\s.JOIN\s+\(SELECT\s+(\w+).*FROM\s+", s):
            Operation = "Join"
            table_info = (
                s.strip()
                .split("JOIN")[-1]
                .split("FROM")[-1]
                .split()[0]
                .replace(")", " ")
                .split()[0]
            )  # split 'CURSOR'
            # print('table_info1', table_info)
            Table_Schema_Name, Table = split_table_info(table_info)
            metadata.append([Operation, Table_Schema_Name, Table])

        if re.search("\s*JOIN\s+\w+", s):
            # print("s:", s)
            Operation = "Join"
            table_info = s.strip().split("JOIN")[-1].split()[0]  # split 'CURSOR'
            # print('table_info', table_info)
            Table_Schema_Name, Table = split_table_info(table_info)
            metadata.append([Operation, Table_Schema_Name, Table])

    return metadata


if __name__ == "__main__":
    os.chdir(INPUT_DIR)
    start = time.time()
    df = pd.DataFrame(
        columns=["File","Procedure_No","Procedure_Schema","Procedure_Name","Commit_No","Operation","Table_Schema_Name","Tables","Sequence"])

    print("\n***** Initiating metadata extraction *********************\n")
    print(f"### Input directory: {INPUT_DIR}")

    for filename in glob.glob("*.sql"):
        fname = filename.replace(".sql", "")
        print(f"\n--- Filename: {fname}")
        file = open(filename, "r")
        sqlFile = file.read()
        file.close()

        sqlCommands = sqlFile.split("\s+END\s+(\w+);")
        print(sqlCommands)  # ("END;")
        sqlCommands = [proc for proc in sqlCommands if proc != "\n\n\n" if proc != ""]

        print("--- Starting metadata extraction ...")
        stored_procedures = split_procedure_to_commits(sqlCommands)

        d2 = []
        pno = 1
        for proc in stored_procedures:
            stored_proc = proc[1]
            procedure = proc[2]
            mydict = {"stored_proc": stored_proc, "procedure": procedure}
            d1 = pd.DataFrame(mydict)
            stored_proc = d1["stored_proc"].tolist()
            procedure = d1["procedure"].tolist()
            # print(f'stored_proc:{len(procedure)}')
            # print(f'stored_proc:{stored_proc}\n procedure:{procedure}')
            # print('==================================================')

            new_list = []
            for sp, p in zip(stored_proc, procedure):
                if isinstance(p, list):
                    for x in p:
                        list1 = [sp, x]
                        new_list.append(list1)
                else:
                    list2 = [sp, p]
                    new_list.append(list2)

            com_no = 1
            for commit in new_list:
                # print(commit[0],'\n----\n', commit[1])
                # print('==============================')
                _, Schema0, Procedure0 = schema_procedure_name(commit[0])
                _, Schema1, Procedure1 = schema_procedure_name(commit[1])
                # print(f'schema0:{Schema0} - procedure0:{Procedure0}\n')
                # print(f'schema1:{Schema1} - procedure1:{Procedure1}\n')

                if (Schema1 == "") & (Procedure1 == ""):
                    result = [
                        [pno] + [Schema0] + [Procedure0] + [com_no] + i
                        for i in procedure_commit_parsing(commit[1])
                    ]
                    # print(f'... Extraction complete for : {Schema0+"_"+Procedure0}')
                    # print('result1:  ', result)
                else:
                    result = [
                        [pno] + [Schema1] + [Procedure1] + [com_no] + i
                        for i in procedure_commit_parsing(commit[1])
                    ]
                    # print('result2:  ', result)
                    # print(f'... Extraction complete for : {Schema1+"_"+Procedure1}')

                d2.extend(result)
                com_no += 1

            pno += 1

        # print(f'... Extraction complete for : {d2[1][1]+"."+d2[1][2]}')

        d2 = pd.DataFrame(
            d2,
            columns=[
                "Procedure_No",
                "Procedure_Schema",
                "Procedure_Name",
                "Commit_No",
                "Operation",
                "Table_Schema_Name",
                "Tables",
            ],
        )
        # print('d2:', d2)
        d2.insert(0, "File", filename)
        d2 = d2[~(d2.Tables.isin(["SET", "UPDATE", "(SELECT", "_DT,"]))]
        d2["Sequence"] = d2.reset_index().index + 1

        # Export data into .csv format
        df = pd.concat([d2])
        df.to_csv(OUTPUT_DIR + "/" + fname + ".csv", index=None)

        print("... Time taken : {:.2f} secs".format(time.time() - start))

    print("***** Metadata extraction complete ****************\n")
