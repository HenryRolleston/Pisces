import pyodbc
import sys
import glob
from dbfread import DBF
import os
from datetime import datetime
from datetime import date

"""
Kia Ora,
Below is the Executable file for 'Pisces', which is a simple Data Migration program I wrote in my second year of
CompSci. On first execution, it will install the Configuration .txt file, and subsequent executions will use the
information in that file to run. The program is simple, and was written before Object Oriented programming (and other
more advanced programming techniques) were taught to me, but functions quickly and accurately.
-Henry.
"""


def get_directory():
    exe_string = sys.executable
    exe_string = exe_string.split("\\")
    return "\\".join(exe_string[:-1:] + [""])


def dbf_sql_uploader(dbf_file, table_name, server, database, uid, password, user_input):
    connection_string = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={{{server}}};'
        f'DATABASE={{{database}}};'
        f'UID={{{uid}}};'
        f'PWD={{{password}}};'
    )

    with pyodbc.connect(connection_string) as connection:
        cursor = connection.cursor()

        if cursor.tables(table=table_name, tableType='Table').fetchone():
            if user_input.lower() in ['o', 'overwrite']:
                print("Table already exists, deleting old version.")
                cursor.execute(f"DROP TABLE {table_name}")
            elif user_input.lower() in ['n', 'new']:
                print("Table already exists, appending today's date to name.")
                heute = str(date.today())
                heute.replace("-", "/")
                table_name += f" {heute}"
            else:
                connection.close()
                print("Table already exists, skipping.")
                return

        dbf_data = DBF(dbf_file, ignore_missing_memofile=True)
        column_names = []
        column_types_dbf = []
        for field in dbf_data.fields:
            column_names.append(field.name)
            column_types_dbf.append(field.type)

        sql_data_types = {
            'C': 'VARCHAR(MAX)',  # Character
            'N': 'NUMERIC(14, 4)',  # Numeric
            'I': 'NUMERIC(14, 4)',  # Integer, needs to be saved as Numeric
            'F': 'FLOAT',  # Float
            'D': 'DATETIME',  # Date
            'T': 'DATETIME',  # DateTime
            'L': 'BIT',  # Logical (Boolean)
            'M': 'TEXT',  # Memo (Text)
            'B': 'VARBINARY(MAX)',  # Binary
        }

        column_types_sql = []
        for column in column_types_dbf:
            column_types_sql.append(sql_data_types[column])
        create_table_query = f'CREATE TABLE [{table_name}] ({", ".join(f"[{col}] {sql_type}" for col, sql_type in zip(column_names, column_types_sql))})'
        cursor.execute(create_table_query)
        row_counter = 0
        cell_integrity_error = False
        today = date.today()
        for record in dbf_data:
            row_counter += 1
            values = [val if val is not None else None for val in record.values()]
            for i in range(len(values)):        ##values vetting
                try:
                    if values[i] == "VALUEERROR":
                        values[i] = None
                        with open(f"{get_directory()}errors.txt", 'a') as f:
                            f.write(f"Error on {today}: {table_name}, row {row_counter}, column {column_names[i]}\n")
                        f.close()
                        cell_integrity_error = True
                    elif column_types_dbf[i] == 'D' and values[i] is not None:
                        temp_datetime = str(values[i]) + " 00:00:00"
                        values[i] = datetime.strptime(temp_datetime, '%Y-%m-%d %H:%M:%S')
                except:
                    values[i] = None
                    with open(f"{get_directory()}errors.txt", 'a') as f:
                        f.write(f"Error on {today}: {table_name}, row {row_counter}, column {column_names[i]}\n")
                    f.close()
                    cell_integrity_error = True

            column_names_escaped = [f"[{col}]" for col in column_names]
            placeholders = ', '.join(['?' for _ in values])
            insert_query = f"INSERT INTO [{table_name}] ({', '.join(column_names_escaped)}) VALUES ({placeholders})"
            try:
                cursor.execute(insert_query, tuple(values))
            except:
                with open(f"{get_directory()}errors.txt", 'a') as f:
                    f.write(f"Error on {today}: {table_name}, row {row_counter}. Check data integrity.\n")
                    cell_integrity_error = True
        if cell_integrity_error:
            print(f"One or more cells in '{table_name}' may have corrupt data. This table will still be uploaded.")
            print(f"The Errors file will have the row/column details.")
        cursor.close()
        connection.commit()
        print(f"Upload of {table_name} complete.")


def foxpro_csv_converter(dbf_file, csv_path):
    import pandas as pd
    pd.set_option('display.max_columns', 20)
    dbf = DBF(dbf_file, ignore_missing_memofile=True)
    df = pd.DataFrame(iter(dbf))
    df.to_csv(csv_path, index=False)


def main():
    first_time = False
    directory = get_directory() + (r'UPLOAD\ '.strip())
    finished_directory = get_directory() + (r'COMPLETED\ '.strip())
    if not os.path.exists(directory):
        first_time = True
    if not os.path.exists(get_directory() + "Pisces Config.txt"):
        first_time = True
    if not os.path.exists(finished_directory):
        first_time = True
    if first_time:
        print("Welcome to Pisces. This software is designed to quickly and easily upload DBF files to a SQL")
        print("Database. Because this is the first time Pisces has been executed from this directory, the")
        print("UPLOAD folder has been created, as well as the Pisces Config file. The UPLOAD folder is where")
        print("you need to place your DBF files, the Pisces Config file contains several settings you may edit,")
        print("including login information and additional functionality. Please place the desired files into")
        print("UPLOAD, and adjust any settings you wish to in Pisces Config before running Pisces again.")
        if not os.path.exists(directory):
            os.makedirs(directory)
        if not os.path.exists(finished_directory):
            os.makedirs(finished_directory)
        if not os.path.exists(get_directory() + "Pisces Config.txt"):
            with open(f"{get_directory()}Pisces Config.txt", 'w') as c:
                c.write(f"Server: REMOVED\n")
                c.write(f"Database: IT_Testing\n")
                c.write(f"Username: REMOVED\n")
                c.write(f"Password: REMOVED\n")
                c.write(f"If Exists? (Overwrite (O), Ignore (I), New Table (N)): O\n")
                c.write(f"Create CSV Backup? (Y/N): Y\n")
                c.write(f"Close on completion? (Y/N): N\n")
                c.write("\n")
                c.write("WARNING: Please do not alter any of the above before the colons.")
    else:
        dbf_list = glob.glob(f"{directory}*.DBF")
        if len(dbf_list) < 1:
            print("Welcome to Pisces. No DBFs found. Please place them in the 'UPLOAD' folder before launching "
                  "Pisces again.")
            return
        print("Welcome to Pisces. Beginning upload.")
        answer_array = []
        config = open(get_directory() + "Pisces Config.txt")
        contents = config.read()
        config.close()
        for line in contents.split("\n")[:7]:
            answer_array.append(line.split(" ")[-1])
        for dbf_file in dbf_list:
            filename = os.path.split(dbf_file)[1]
            try:
                print(f"Uploading {filename}.")
                dbf_sql_uploader(dbf_file, filename[:-4], answer_array[0], answer_array[1], answer_array[2],
                                 answer_array[3], answer_array[4])
                if answer_array[5].lower() in ["y", "yes"]:
                    csv_directory = get_directory() + ("CSV Output\ ").strip()
                    if not os.path.exists(csv_directory):
                        os.makedirs(csv_directory)
                    current_name = os.path.split(dbf_file)[1]
                    csv_path = csv_directory + current_name[:-4] + ".csv"
                    print(f"Beginning CSV conversion of {current_name}")
                    foxpro_csv_converter(dbf_file, csv_path)
                    print(f"CSV conversion of {current_name} complete.")
                current_name = os.path.split(dbf_file)[1]
                os.replace(dbf_file, finished_directory + current_name)
            except Exception as e:
                today = date.today()
                with open(f"{get_directory()}errors.txt", 'a') as f:
                    f.write(f"{today}: Table: {filename}, {e}\n")
                f.close()
                print(f"A Serious Error occurred while uploading {filename}. Check 'errors.txt'. This table could "
                      f"not be uploaded. Contact IT.")
        if answer_array[6].lower() in ['y', 'yes']:
            sys.exit()


main()
input("Enter to exit: ")