import sqlite3
import mysql.connector
from mysql.connector import errorcode
import os
import re
import tempfile
import traceback

# --- MySQL Connection Details (using your latest provided) ---
MYSQL_HOST = ""
MYSQL_PORT = 
MYSQL_USER = ""
MYSQL_PASSWORD = ""
MYSQL_DBNAME = ""
MYSQL_CHARSET = "utf8mb4"
MYSQL_COLLATION = "utf8mb4_unicode_ci"
# --- End MySQL Connection Details ---

# --- SQLite Input File ---
SQLITE_DUMP_FILE = 'export.sql'
# --- End SQLite Input File ---

BOOLEAN_COLUMNS_MAP = {
    'entities': { 'type': None, 'reference_count': '1' },
    'files': { 'type': None, 'is_symbolic': 'false' },
    'metadata': { 'is_public': 'false' }
}

# --- Type Mapping and Helper Functions for MySQL ---
def get_mysql_column_type(sqlite_type, column_name, table_name, is_pk_from_pragma):
    sqlite_type_upper = sqlite_type.upper() if sqlite_type else "TEXT"

    if (table_name in BOOLEAN_COLUMNS_MAP and column_name in BOOLEAN_COLUMNS_MAP[table_name]) or \
       sqlite_type_upper == "BOOL":
        return "TINYINT(1)"

    if "INT" in sqlite_type_upper and column_name.lower() == 'id' and is_pk_from_pragma:
        return "BIGINT AUTO_INCREMENT"

    if "INT" in sqlite_type_upper: return "BIGINT"
    if sqlite_type_upper in ["REAL", "FLOAT", "DOUBLE"]: return "DOUBLE"

    if sqlite_type_upper == "TEXT": return "LONGTEXT"
    if sqlite_type_upper == "BLOB": return "LONGBLOB"

    if sqlite_type_upper == "DATETIME":
        return "TIMESTAMP(6)"

    if sqlite_type_upper == "JSON": return "JSON"

    if sqlite_type_upper == "UUID": return "CHAR(36)"

    print(f"Warning: Unhandled SQLite type '{sqlite_type}' for {table_name}.{column_name}. Defaulting to LONGTEXT.")
    return "LONGTEXT"


def get_mysql_default_value(sqlite_default, mysql_type, column_name, table_name, is_col_not_null): # Added is_col_not_null
    mysql_type_upper = mysql_type.upper()

    if "AUTO_INCREMENT" in mysql_type_upper:
        return None

    # Handle disallowed defaults for TEXT/BLOB/JSON/GEOMETRY
    # These types cannot have a literal default value other than explicit NULL (if nullable)
    if mysql_type_upper in ["TEXT", "LONGTEXT", "MEDIUMTEXT", "TINYTEXT",
                            "BLOB", "LONGBLOB", "MEDIUMBLOB", "TINYBLOB",
                            "JSON"] or "GEOMETRY" in mysql_type_upper:
        if sqlite_default is None and not is_col_not_null: # If SQLite default was NULL and col is nullable
            return "NULL" # Explicitly DEFAULT NULL for these types if SQLite default was NULL
        else:
            # If SQLite had a non-NULL default, or if col is NOT NULL (and thus can't default to NULL easily for these types)
            # we must OMIT the DEFAULT clause for MySQL.
            if sqlite_default is not None:
                 print(f"Warning: MySQL type {mysql_type} for {table_name}.{column_name} cannot have literal default '{sqlite_default}'. Omitting DEFAULT clause.")
            return None # Omit DEFAULT clause

    if mysql_type_upper == "TINYINT(1)": # Boolean
        if sqlite_default is not None:
            val_lower = str(sqlite_default).lower().strip("'")
            if val_lower in ['1', 'true']: return "1"
            if val_lower in ['0', 'false']: return "0"
        # If no explicit true/false default from SQLite, and column is NOT NULL,
        # MySQL would default TINYINT to 0. If nullable, defaults to NULL.
        # Returning None lets the DDL omit DEFAULT unless explicitly NULL for nullable.
        if sqlite_default is None and not is_col_not_null: return "NULL"
        return None # Let MySQL handle its own implicit default if not explicitly NULL

    if sqlite_default is None:
        return "NULL" if not is_col_not_null else None # Only DEFAULT NULL if nullable

    # Numeric defaults
    if mysql_type_upper.startswith("BIGINT") or mysql_type_upper.startswith("INT") or \
       mysql_type_upper.startswith("DOUBLE") or mysql_type_upper.startswith("DECIMAL"):
        try:
            temp_val = str(sqlite_default).strip("'\"")
            float(temp_val)
            return temp_val
        except ValueError:
            print(f"Warning: Could not parse default '{sqlite_default}' as number for {table_name}.{column_name}.")
            return None

    # Text (CHAR/VARCHAR), JSON defaults need to be quoted strings
    # This block now primarily handles CHAR/VARCHAR, as TEXT/LONGTEXT defaults are handled above.
    # JSON was also handled above for non-NULL defaults.
    if mysql_type_upper.startswith("CHAR") or mysql_type_upper.startswith("VARCHAR"):
        val = str(sqlite_default)
        escaped_val = val.replace("'", "''")
        if val.startswith("'") and val.endswith("'") and len(val) > 1:
            escaped_val = val[1:-1].replace("'", "''")
        return f"'{escaped_val}'"

    # TIMESTAMP / DATETIME defaults
    if mysql_type_upper.startswith("TIMESTAMP") or mysql_type_upper.startswith("DATETIME"):
        val_upper = str(sqlite_default).upper().strip("'")
        if val_upper == "CURRENT_TIMESTAMP" or val_upper == "NOW()":
            # MySQL 8.0.13+ allows fractional seconds for CURRENT_TIMESTAMP default
            precision = ""
            if "(6)" in mysql_type: precision = "(6)"
            # For older MySQL, CURRENT_TIMESTAMP on TIMESTAMP col implies ON UPDATE CURRENT_TIMESTAMP too.
            # This logic assumes newer MySQL or that behavior is acceptable.
            return f"CURRENT_TIMESTAMP{precision}"

        try:
            processed_dt = preprocess_mysql_datetime_string(str(sqlite_default))
            if processed_dt:
                return f"'{processed_dt}'"
        except Exception as e_dt_default:
            print(f"Warning: Could not process datetime default '{sqlite_default}' for {table_name}.{column_name}: {e_dt_default}")
            pass

    print(f"Warning: Default '{sqlite_default}' for MySQL type {mysql_type} on {table_name}.{column_name} (NOT NULL: {is_col_not_null}) not translated well.")
    return None


def preprocess_mysql_datetime_string(value_str):
    if not isinstance(value_str, str) or not value_str.strip():
        return None

    cleaned_value = re.sub(r"\s+m=[\+\-]\d+\.\d+$", "", value_str).strip()
    match_dt_offset = re.match(r"^(.*?)(?:\s?([+\-]\d{2}:?\d{2}(?::?\d{2}(?:\.\d+)?)?|Z))(?:\s+[A-Z_a-z]+)?$", cleaned_value)

    datetime_part_to_format = cleaned_value
    if match_dt_offset:
        datetime_part_to_format = match_dt_offset.group(1).strip()
    elif cleaned_value.endswith('Z'):
        datetime_part_to_format = cleaned_value[:-1].strip()

    datetime_part_to_format = datetime_part_to_format.replace("T", " ")

    if '.' in datetime_part_to_format:
        base, frac = datetime_part_to_format.split('.', 1)
        frac_digits_only = "".join(filter(str.isdigit, frac))
        frac_final = frac_digits_only[:6].ljust(6, '0') if frac_digits_only else "000000"
        datetime_part_to_format = f"{base}.{frac_final}"
    else:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", datetime_part_to_format):
            datetime_part_to_format = f"{datetime_part_to_format} 00:00:00.000000"
        elif re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", datetime_part_to_format): # No frac part
             datetime_part_to_format = f"{datetime_part_to_format}.000000"
        # else: could be an invalid format, let MySQL handle it or add more parsing

    return datetime_part_to_format


# --- Main Migration Logic ---
def migrate_data():
    mysql_conn = None
    sqlite_conn = None
    db_fd, temp_sqlite_db_path = tempfile.mkstemp(suffix=".sqlite")
    os.close(db_fd)
    print(f"Using temporary SQLite database: {temp_sqlite_db_path}")
    migrated_tables_info = []

    try:
        print(f"Loading SQLite dump from '{SQLITE_DUMP_FILE}' into temporary DB...")
        sqlite_conn = sqlite3.connect(temp_sqlite_db_path)
        sqlite_cursor = sqlite_conn.cursor()
        with open(SQLITE_DUMP_FILE, 'r', encoding='utf-8', errors='replace') as f_dump:
            sql_script = f_dump.read()
            sqlite_cursor.executescript(sql_script)
            sqlite_conn.commit()
        print("SQLite dump loaded successfully.")

        print(f"Connecting to MySQL database '{MYSQL_DBNAME}' on {MYSQL_HOST}:{MYSQL_PORT}...")
        mysql_conn = mysql.connector.connect(
            host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD,
            database=MYSQL_DBNAME, charset=MYSQL_CHARSET, use_unicode=True
        )
        mysql_cursor = mysql_conn.cursor()
        print("Connected to MySQL successfully.")

        sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
        tables = [row[0] for row in sqlite_cursor.fetchall()]
        if not tables:
            print("No user tables found in SQLite.")
            return []
        print(f"Found tables in SQLite: {', '.join(tables)}")

        for table_name in tables:
            print(f"\n--- Processing table: {table_name} ---")
            mysql_safe_table_name = f"`{table_name}`"

            sqlite_cursor.execute(f'PRAGMA table_info("{table_name}");')
            schema_info = sqlite_cursor.fetchall()
            if not schema_info:
                print(f"Could not get schema for {table_name}. Skipping.")
                continue

            column_definitions = []
            pk_column_tuples = []
            column_names_ordered_from_pragma = []
            table_has_auto_increment_id = False

            for col_pragma in schema_info:
                col_name, col_type_sqlite, col_notnull_flag, col_dflt_val_sqlite, col_pk_order = col_pragma[1], col_pragma[2], col_pragma[3], col_pragma[4], col_pragma[5]
                mysql_safe_col_name = f"`{col_name}`"
                column_names_ordered_from_pragma.append(col_name)

                is_part_of_pk = (col_pk_order > 0)
                is_sqlite_col_not_null = (col_notnull_flag == 1)
                mysql_col_type = get_mysql_column_type(col_type_sqlite, col_name, table_name, is_part_of_pk)

                col_def = f"{mysql_safe_col_name} {mysql_col_type}"
                if "AUTO_INCREMENT" in mysql_col_type.upper():
                    table_has_auto_increment_id = True
                else:
                    if is_sqlite_col_not_null: col_def += " NOT NULL"

                    # Pass is_sqlite_col_not_null to decide if DEFAULT NULL is appropriate
                    mysql_default = get_mysql_default_value(col_dflt_val_sqlite, mysql_col_type, col_name, table_name, is_sqlite_col_not_null)
                    if mysql_default is not None:
                        # Only add DEFAULT NULL if column is nullable and SQLite default was NULL
                        if mysql_default == "NULL" and not is_sqlite_col_not_null:
                            col_def += " DEFAULT NULL"
                        elif mysql_default != "NULL": # For any other non-NULL default value
                            col_def += f" DEFAULT {mysql_default}"
                        # If mysql_default is "NULL" but col is NOT NULL, DEFAULT clause is omitted.
                        # If mysql_default is None (e.g. for TEXT), DEFAULT clause is omitted.

                column_definitions.append(col_def)
                if is_part_of_pk :
                    pk_column_tuples.append((col_pk_order, mysql_safe_col_name))

            print(f"  Dropping and Creating table {mysql_safe_table_name} in MySQL...")
            try:
                mysql_cursor.execute(f'SET FOREIGN_KEY_CHECKS=0;')
                mysql_cursor.execute(f'DROP TABLE IF EXISTS {mysql_safe_table_name};')
            except mysql.connector.Error as e_drop:
                print(f"    Warning: Could not drop table {mysql_safe_table_name} (may not exist): {e_drop}")
            finally:
                 mysql_cursor.execute(f'SET FOREIGN_KEY_CHECKS=1;')

            create_table_sql = f'CREATE TABLE {mysql_safe_table_name} (\n  ' + ",\n  ".join(column_definitions)
            if pk_column_tuples:
                pk_column_tuples.sort(key=lambda x: x[0])
                sorted_pk_col_names = [col_name_quoted for pk_order, col_name_quoted in pk_column_tuples]
                create_table_sql += f",\n  PRIMARY KEY ({', '.join(sorted_pk_col_names)})"

            create_table_sql += f"\n) ENGINE=InnoDB CHARACTER SET={MYSQL_CHARSET} COLLATE={MYSQL_COLLATION};"
            mysql_cursor.execute(create_table_sql) # This is where the error occurred
            print(f"  Table {mysql_safe_table_name} created.")

            print(f"  Transferring data for table {mysql_safe_table_name}...")
            select_cols_str_sqlite = ", ".join([f'"{c}"' for c in column_names_ordered_from_pragma])
            sqlite_cursor.execute(f'SELECT {select_cols_str_sqlite} FROM "{table_name}";')

            insert_cols_str_mysql = ", ".join([f"`{c}`" for c in column_names_ordered_from_pragma])
            placeholders_str = ", ".join(["%s"] * len(column_names_ordered_from_pragma))
            insert_sql_template = f'INSERT INTO {mysql_safe_table_name} ({insert_cols_str_mysql}) VALUES ({placeholders_str});'

            rows_processed = 0
            for sqlite_row_tuple in sqlite_cursor:
                mysql_row_values = list(sqlite_row_tuple)
                for i, col_name in enumerate(column_names_ordered_from_pragma):
                    original_sqlite_type = ""
                    is_pk_col_runtime = False
                    for sch_col_info in schema_info:
                        if sch_col_info[1] == col_name:
                            original_sqlite_type = sch_col_info[2]
                            is_pk_col_runtime = (sch_col_info[5] > 0)
                            break

                    mysql_target_type_for_col = get_mysql_column_type(original_sqlite_type, col_name, table_name, is_pk_col_runtime)
                    current_val = mysql_row_values[i]

                    if mysql_target_type_for_col == "TINYINT(1)":
                        if current_val == 1 or (isinstance(current_val, str) and current_val.lower() in ['true', 't', '1']):
                            mysql_row_values[i] = 1
                        elif current_val == 0 or (isinstance(current_val, str) and current_val.lower() in ['false', 'f', '0']):
                            mysql_row_values[i] = 0
                        elif current_val is None:
                            mysql_row_values[i] = None

                    elif mysql_target_type_for_col.upper().startswith("TIMESTAMP") or \
                         mysql_target_type_for_col.upper().startswith("DATETIME"):
                        mysql_row_values[i] = preprocess_mysql_datetime_string(current_val)

                    elif mysql_target_type_for_col == "JSON" and isinstance(current_val, bytes):
                        try:
                            try: decoded_string = current_val.decode('utf-8')
                            except UnicodeDecodeError:
                                print(f"Warning: UTF-8 decode failed for JSON bytes in {table_name}.{col_name}. Trying with 'replace'. Value: {repr(current_val)}")
                                decoded_string = current_val.decode('utf-8', 'replace')
                            mysql_row_values[i] = decoded_string
                        except Exception as e_json_decode:
                             print(f"Error decoding bytes for JSON in {table_name}.{col_name}: {e_json_decode}. Value: {repr(current_val)}. Setting to NULL.")
                             mysql_row_values[i] = None

                    elif isinstance(current_val, bytes) and \
                         ("TEXT" in mysql_target_type_for_col.upper() or "CHAR" in mysql_target_type_for_col.upper()):
                        try:
                            mysql_row_values[i] = current_val.decode('utf-8', 'replace')
                        except Exception as e_text_decode:
                            print(f"Error decoding bytes for TEXT/CHAR in {table_name}.{col_name}: {e_text_decode}. Value: {repr(current_val)}. Setting to NULL.")
                            mysql_row_values[i] = None

                try:
                    mysql_cursor.execute(insert_sql_template, tuple(mysql_row_values))
                    rows_processed += 1
                except mysql.connector.Error as e_insert:
                    print(f"\n!!! ERROR inserting row into table {mysql_safe_table_name} !!!")
                    print(f"    SQL Template: {insert_sql_template}")
                    print(f"    Problematic MySQL-bound values (len {len(mysql_row_values)}): {mysql_row_values}")
                    print(f"    Original SQLite row values (len {len(sqlite_row_tuple)}): {sqlite_row_tuple}")
                    print(f"    Column names for insert (len {len(column_names_ordered_from_pragma)}): {column_names_ordered_from_pragma}")
                    print(f"    Insert error details: {e_insert} (Code: {e_insert.errno})")
                    if e_insert.errno == errorcode.ER_TRUNCATED_WRONG_VALUE_FOR_FIELD:
                        print("    This might be a data type mismatch or encoding issue for a specific field.")
                    mysql_conn.rollback()
                    raise

            mysql_conn.commit()
            print(f"  Transferred {rows_processed} rows for table {mysql_safe_table_name}.")
            if table_has_auto_increment_id:
                migrated_tables_info.append((table_name, True))
            else:
                migrated_tables_info.append((table_name, False))

        print("\nMigration process completed successfully.")
        return migrated_tables_info

    except mysql.connector.Error as db_err:
        print(f"\n!!! MYSQL DATABASE ERROR OCCURRED: {db_err} !!!")
        if 'mysql_conn' in locals() and mysql_conn and mysql_conn.is_connected(): mysql_conn.rollback()
        traceback.print_exc()
        return []
    except sqlite3.Error as sqlite_err:
        print(f"\n!!! SQLITE DATABASE ERROR OCCURRED: {sqlite_err} !!!")
        traceback.print_exc()
        return []
    except Exception as e:
        print(f"\n!!! AN UNEXPECTED ERROR OCCURRED: {e} !!!")
        if 'mysql_conn' in locals() and mysql_conn and mysql_conn.is_connected(): mysql_conn.rollback()
        traceback.print_exc()
        return []
    finally:
        if 'mysql_cursor' in locals() and mysql_cursor: mysql_cursor.close()
        if 'mysql_conn' in locals() and mysql_conn and mysql_conn.is_connected():
            mysql_conn.close()
            print("MySQL connection closed after migration.")
        if 'sqlite_cursor' in locals() and sqlite_cursor: sqlite_cursor.close()
        if 'sqlite_conn' in locals() and sqlite_conn: sqlite_conn.close(); print("SQLite connection closed.")
        if os.path.exists(temp_sqlite_db_path):
            try:
                os.remove(temp_sqlite_db_path)
                print(f"Temporary SQLite database '{temp_sqlite_db_path}' removed.")
            except OSError as e_remove:
                print(f"Error removing temp SQLite DB '{temp_sqlite_db_path}': {e_remove}")


def reset_mysql_auto_increment(processed_tables_info):
    if not processed_tables_info:
        print("No tables processed or no auto_increment info, skipping auto_increment reset.")
        return

    print("\n--- Attempting to reset MySQL AUTO_INCREMENT values for relevant tables ---")
    mysql_conn_reset = None
    try:
        mysql_conn_reset = mysql.connector.connect(
            host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
            password=MYSQL_PASSWORD, database=MYSQL_DBNAME, charset=MYSQL_CHARSET
        )
        mysql_cursor_reset = mysql_conn_reset.cursor()

        for table_name, has_auto_inc_id in processed_tables_info:
            if has_auto_inc_id:
                mysql_safe_table_name_reset = f"`{table_name}`"
                pk_col_name_mysql_safe = "`id`"
                try:
                    mysql_cursor_reset.execute(f'SELECT COALESCE(MAX({pk_col_name_mysql_safe}), 0) FROM {mysql_safe_table_name_reset};')
                    max_id_row = mysql_cursor_reset.fetchone()
                    max_id = max_id_row[0] if max_id_row else 0
                    next_auto_inc_val = max_id + 1

                    print(f"    Setting AUTO_INCREMENT for {mysql_safe_table_name_reset} to {next_auto_inc_val}.")
                    mysql_cursor_reset.execute(f"ALTER TABLE {mysql_safe_table_name_reset} AUTO_INCREMENT = %s;", (next_auto_inc_val,))
                    print(f"    AUTO_INCREMENT for {mysql_safe_table_name_reset} set successfully.")
                except mysql.connector.Error as e_seq:
                    print(f"    Error setting AUTO_INCREMENT for {mysql_safe_table_name_reset}: {e_seq}")
            # else:
            #     print(f"  Table '{table_name}' not flagged for auto_increment reset. Skipping.")
    except mysql.connector.Error as e:
        print(f"MySQL Error during AUTO_INCREMENT reset: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during AUTO_INCREMENT reset: {e}")
        traceback.print_exc()
    finally:
        if 'mysql_cursor_reset' in locals() and mysql_cursor_reset: mysql_cursor_reset.close()
        if 'mysql_conn_reset' in locals() and mysql_conn_reset and mysql_conn_reset.is_connected():
            mysql_conn_reset.close()
            print("MySQL connection closed after auto_increment reset.")

if __name__ == '__main__':
    if not os.path.exists(SQLITE_DUMP_FILE):
        print(f"Error: SQLite dump file '{SQLITE_DUMP_FILE}' not found.")
    else:
        processed_tables_info_list = migrate_data()
        # Ensure reset is called even if processed_tables_info_list is empty, as long as migration ran
        if processed_tables_info_list is not None:
            reset_mysql_auto_increment(processed_tables_info_list)
        else:
            print("Migration failed very early or did not run, skipping auto_increment reset.")