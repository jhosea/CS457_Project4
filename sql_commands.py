import os
import shutil
import json

import pandas as pd
import numpy as np
import operator

DATABASE_DIR = 'databases'

class Invalid_Command(Exception):
    '''Exception for when a command is invalid'''
    pass


def format_values(value):
    '''
    This function takes in a string and formats it correctly as a string, float, or int.

    Returns: value with correct datatype
    '''
    # Looks for quotes and removes them - keeps as string
    if "'" in value:
        value = value[value.find("'")+1:value.rfind("'")]
    
    # If no quotes assumes numnber and checks for period then converts to float
    elif '.' in value and str.isdigit(value.replace('.','')):
        value = float(value)

    # If no period, converts to int
    elif str.isdigit(value):
        value = int(value)
    
    return value


def create(command, database, **kwargs):
    '''
    Function checks if we are creating a table or database and returns corresponding function
    '''
    # Checks if command is long enough
    if len(command) < 3:
        raise Invalid_Command('Invalid Command entered.\n')

    # Checks if creating database or table
    if command[1] == 'table':
        return create_table(command, database)
    elif command[1] == 'database':
        return create_database(command, database)
    else:
        raise Invalid_Command('Can only create a database or table.\n')


def drop(command, database, **kwargs):
    '''
    Function checks if we are dropping a table or database and returns corresponding function
    '''
    # Checks if command is long enough
    if len(command) < 3:
        raise Invalid_Command('Invalid Command entered.\n')

    # Checks if dropping database or table
    if command[1] == 'table':
        return drop_table(command, database)
    elif command[1] == 'database':
        return drop_database(command, database)
    else:
        raise Invalid_Command('Can only drop a database or table.\n')


def create_database(command, database, **kwargs):
    '''
    Function checks if the main databases directory exists and creates it if not.
    Then creates a folder for the specified data database

    Returns: None
    '''
    if not os.path.isdir(DATABASE_DIR):
        os.mkdir(DATABASE_DIR)

    database_name = command[2] # Extracts database name
    database_path = os.path.join(DATABASE_DIR,database_name)

    if not os.path.isdir(database_path):
        os.mkdir(database_path) # Creates database folder
    else:
        raise Invalid_Command(f'Failed to create database {database_name} because it already exists.\n')

    print(f'Database {database_name} created.\n')

    return database


def create_table(command, database, **kwargs):
    '''
    Function checks if the table exists and creates the table (stores as csv) and schema (stores as json)

    Returns: database name
    '''

    if database == '':
        raise Invalid_Command('No database specified.\n')
    
    table_name = command[2]
    if '(' in table_name:
        table_name = table_name.split('(')[0]
    table_path = os.path.join(DATABASE_DIR,database,f'{table_name}.csv')

    # Checks if table exists
    if not os.path.isfile(table_path):

        # Rejoins the command string and finds the first and last parenthesis for the table schema
        string_command = ' '.join(command)
        table_schema = string_command[string_command.find('(')+1:string_command.rfind(')')]

        # Splits the schema into columns using commas
        command_schema = table_schema.split(',')
        table_schema = dict()

        # Loops through each column and extracts the name and data type and store them in table_schema dict
        for col in command_schema:
            col = col.strip()
            try:
                col_name = col.split(' ')[0]
                col_type = col.split(' ')[1]

            except IndexError:
                raise Invalid_Command('No column type provided.\n')
            table_schema[col_name] = col_type

        # Creates empty dataframe with specified columns
        table = pd.DataFrame(columns = table_schema.keys())
        # Exports table DataFrame to csv in the database folder
        table.to_csv(table_path, index = False)

        # Exports schema dictionary to JSON file in the database folder
        with open(os.path.join(DATABASE_DIR,database,f'{table_name}_schema.json'),'w') as f:
            json.dump(table_schema,f)

    # If the table already exists raise exception
    else:
        raise Invalid_Command(f'Failed to create table {table_name} because it already exists.\n')

    print(f'Table {table_name} created.\n')

    # Returns database so we can continue using it
    return database


def drop_database(command, database, **kwargs):
    '''
    Function checks if databases exists and deletes it.

    Returns: None
    '''
    database_name = command[2] # Extracts database name
    database_path = os.path.join(DATABASE_DIR,database_name)

    if os.path.isdir(database_path):
        shutil.rmtree(database_path)
    else:
        raise Invalid_Command(f'Failed to delete database {database_name} because it does not exists.\n')

    print(f'Database {database_name} deleted.\n')

    return database


def drop_table(command, database, **kwargs):
    '''
    Function checks if table exists and deletes it.

    Returns: database name
    '''
    table_name = command[2] # Extracts database name
    table_path = os.path.join(DATABASE_DIR,database,f'{table_name}.csv')
    schema_path = os.path.join(DATABASE_DIR,database,f'{table_name}_schema.json')

    # Checks if csv and json file exist and deletes both
    if os.path.isfile(table_path) & os.path.isfile(schema_path):
        os.remove(table_path)
        os.remove(schema_path)
    else:
        raise Invalid_Command(f'Failed to delete table {table_name} because it does not exists.\n')

    print(f'Table {table_name} deleted.\n')

    # Returns database so we can continue using it
    return database


def use_database(command,**kwargs):
    # Function checks if database exists and returns database name

    # Checks if command is long enough
    if len(command) < 2:
        raise Invalid_Command('Invalid Command entered.\n')

    database_name = command[1]
    # Checks if database exists
    if os.path.isdir(os.path.join(DATABASE_DIR,database_name)):
        print(f'Using database {database_name}.\n')
        return database_name
    else:
        raise Invalid_Command(f'Could not find database {database_name}.\n')


def select_command(command, database, raw_command, **kwargs):
    '''
    Function checks if the table exists then selects the columns specified

    Returns: database name
    '''

    if not database:
        raise Invalid_Command('Not database selected.\n')
    if len(command) < 4:
        raise Invalid_Command('Command is not valid.')
    # Uses select and from to find columns and table name
    try:
        return_cols = command[command.index('select')+1:command.index('from')]
    except ValueError:
        raise Invalid_Command('No SELECT or FROM found in command.\n')

    # Parses FROM statement
    if 'where' in command:
        from_statement = command[command.index('from')+1:command.index('where')]
    else:
        from_statement = command[command.index('from')+1:]
    
    # Checks for join in FROM and if found joins the tables
    if 'join' in from_statement:

        # Finds table names and aliases
        left_table_name = from_statement[0]
        left_table_alias = from_statement[1]

        right_table_name = from_statement[from_statement.index('join')+1]
        right_table_alias = from_statement[from_statement.index('join')+2]

        # Finds and formats join type
        join_type = from_statement[2:from_statement.index('join')]
        
        if 'outer' in join_type and len(join_type) > 1:
            join_type.remove('outer')

        join_type = join_type[0]

        # Finds join keys
        on_key = from_statement[from_statement.index('on')+1:]

        if on_key[0].split('.')[0] == left_table_alias:
            left_key = on_key[0]
            right_key = on_key[2]
        
        else:
            left_key = on_key[2]
            right_key = on_key[0]

        
        # Creates table and schema path
        left_table_path = os.path.join(DATABASE_DIR,database,f'{left_table_name}.csv')
        right_table_path = os.path.join(DATABASE_DIR,database,f'{right_table_name}.csv')

        # Checks if table exists
        if os.path.isfile(left_table_path) and os.path.isfile(right_table_path):
            left_table_df = pd.read_csv(left_table_path)
            right_table_df = pd.read_csv(right_table_path)
        
        else:
            raise Invalid_Command("Could not find table.")

        # Adds table alias as prefix
        left_table_df = left_table_df.add_prefix(left_table_alias + '.')
        right_table_df = right_table_df.add_prefix(right_table_alias + '.')

        # Creates cross join
        if join_type == 'cross':

            # Creates index of only zero
            left_table_df.index = [0]*len(left_table_df)
            right_table_df.index = [0]*len(right_table_df)

            table_df = left_table_df.merge(right=right_table_df, left_index=True, right_index=True)
            
        else:
            table_df = left_table_df.merge(right=right_table_df, how=join_type, left_on=left_key, right_on=right_key)


    # If there is no join then check for a comma indicating two tables
    elif ',' in ' '.join(from_statement):
        
        left_table_name = from_statement[0]
        left_table_alias = from_statement[1].replace(',','')

        right_table_name = from_statement[2]
        right_table_alias = from_statement[3]

        # Creates table and schema path
        left_table_path = os.path.join(DATABASE_DIR,database,f'{left_table_name}.csv')
        right_table_path = os.path.join(DATABASE_DIR,database,f'{right_table_name}.csv')

        # Checks if table exists
        if os.path.isfile(left_table_path) and os.path.isfile(right_table_path):
            left_table_df = pd.read_csv(left_table_path)
            right_table_df = pd.read_csv(right_table_path)
        
        else:
            raise Invalid_Command("Could not find table.")

        # Mimics cross join and uses WHERE to filter
        left_table_df.index = [0]*len(left_table_df)
        right_table_df.index = [0]*len(right_table_df)

        # Adds table alias as prefix
        left_table_df = left_table_df.add_prefix(left_table_alias + '.')
        right_table_df = right_table_df.add_prefix(right_table_alias + '.')

        table_df = left_table_df.merge(right=right_table_df, left_index=True, right_index=True)
    

    # If there is no comma or join then use the first token after the FROM
    else:
        table_name = from_statement[0]
        table_path = os.path.join(DATABASE_DIR,database,f'{table_name}.csv')
        table_df = pd.read_csv(table_path)

    # Checks if table exists
    if table_df is not None:


        # Runs if there was a where command
        where_command = raw_command[raw_command.lower().find('where'):]
        if 'where' in where_command.lower():
            where_command = where_command.replace(';','').split(' ')
            where_command = [x.strip() for x in where_command]
            where_command = [x for x in where_command if x != '']
        
            filter_series = where(where_command=where_command, table_df=table_df)

            table_df = table_df.loc[filter_series]

        # Returns all columns if *
        if return_cols == ['*']:
            print(table_df, '\n')

        # Else split columns with comma and create a list of column names
        else:
            return_cols = ' '.join(return_cols)
            return_cols = return_cols.split(',')
            return_cols = [x.strip() for x in return_cols]

            # Checks if all columns exist
            if all(col_name in table_df.columns for col_name in return_cols):

                # Prints all selected columns
                print(table_df[return_cols], '\n')
            else:
                raise Invalid_Command(f'Table at least one specified column was not found.\n')
    # Raises error if table not found
    else:
        raise Invalid_Command(f'Table {table_df[0]} not found.\n')

    # Returns database so we can continue using it
    return database


def alter_table(command, database, **kwargs):
    '''
    Function adds or removes one column from the table.
    First checks if the table exists then calls the add or remove function.

    Returns: database name
    '''
    # Checks if second word is table and the command is long enough
    if len(command) < 5 or command[1] != 'table':
        raise Invalid_Command('Alter command is invalid.\n')

    # Extracts table name, add or remove and column name
    table_name = command[2]
    add_or_remove = command[3]
    col_name = command[4]

    # Creates table and schema path
    table_path = os.path.join(DATABASE_DIR,database,f'{table_name}.csv')
    schema_path = os.path.join(DATABASE_DIR,database,f'{table_name}_schema.json')

    # Checks if table exists
    if os.path.isfile(table_path) & os.path.isfile(schema_path):

        # Checks if add and if so selects the column type
        if add_or_remove == 'add':
            if len(command) < 6:
                raise Invalid_Command('Alter add command is invalid.\n')
            col_type = command[5]
            add_to_table(table_path, schema_path, col_name, col_type)
            print(f'Added column {col_name} {col_type} to {table_name}.\n')

        # Checks if remove and calls command if so
        elif add_or_remove == 'remove':
            remove_from_table(table_path, schema_path, col_name)
            print(f'Removed column {col_name} from {table_name}.\n')

        else:
            raise Invalid_Command('Alter command can only add or remove columns.\n')

    else:
        raise Invalid_Command(f'Could not find table {table_name}.\n')

    return database


def add_to_table(table_path, schema_path, column_name, column_type, **kwargs):
    '''
    This function checks if a column exists and if not adds the column to the table and schema.

    Returns: None
    '''
    # Loads table and check if column exists
    table = pd.read_csv(table_path)
    if column_name in table.columns:
        raise Invalid_Command(f'Column {column_name} already exists.\n')

    # Loads schema
    with open(schema_path,'r') as f:
        schema = json.load(f)

    # Adds new column to table and saves table to csv
    table[column_name] = np.nan
    table.to_csv(table_path, index = False)

    # Adds new column to schema and saves to json
    schema[column_name] = column_type
    with open(schema_path,'w') as f:
        json.dump(schema,f)

    return None


def remove_from_table(table_path, schema_path, column_name, **kwargs):
    '''
    This function checks if a column exists and if so deletes the column to the table and schema.

    Returns: None
    '''
    # Loads table and check if column exists
    table = pd.read_csv(table_path)
    if column_name not in table.columns:
        raise Invalid_Command(f'Column {column_name} does not exists.\n')

    # Loads schema
    with open(schema_path,'r') as f:
        schema = json.load(f)

    # Drops columm from table and saves to csv
    table = table.drop(columns = column_name)
    table.to_csv(table_path, index = False)

    # Drops column from schema and saves to json
    schema.pop(column_name)
    with open(schema_path,'w') as f:
        json.dump(schema,f)

    return None


def insert(command, database, raw_command, **kwargs):
    '''
    This function inserts values to a specified table.

    Returns: database name
    '''

    # Checks for database and INTO keyword
    if database == '':
        raise Invalid_Command('No database specified.\n')

    if command[1] != 'into':
        raise Invalid_Command('Missing "INTO" keyword.\n')
    
    # Gets table name and path
    table_name = command[2]
    table_path = os.path.join(DATABASE_DIR,database,f'{table_name}.csv')

    # Checks if table exists
    if os.path.isfile(table_path):

        # Looks for values keyword
        try:
            values_to_insert = raw_command[raw_command.find('values'):]
        
        except:
            raise Invalid_Command('Could not find keyword "VALUES".\n')

        # Extracts values to insert and splits on comma
        values_to_insert = values_to_insert[values_to_insert.find('(')+1:values_to_insert.rfind(')')].split(',')

        # Checks for quote or period to indicate value is a float or a string. Else will be converted to an int.
        formatted_values = list()
        for value in values_to_insert:
            # Correcly formats value and appends it
            formatted_values.append(format_values(value))

        # Reads table, adds new row and saves back in csv
        table_df = pd.read_csv(table_path)

        table_df.loc[table_df.index.max()+1] = formatted_values

        table_df.to_csv(table_path, index = False)


    # If the table already exists raise exception
    else:
        raise Invalid_Command(f'Failed to insert. Table {table_name} could not be found.\n')

    print(f'1 new record inserted into table {table_name}.\n')

    # Returns database so we can continue using it
    return database


def delete(command, database, raw_command, **kwargs):
    '''
    This function rows from a specified table.

    Returns: database name
    '''
    # Checks for database and INTO keyword
    if database == '':
        raise Invalid_Command('No database specified.\n')

    if command[1] != 'from':
        raise Invalid_Command('Missing "FROM" keyword.\n')
    
    # Gets table name and path
    table_name = command[2]
    table_path = os.path.join(DATABASE_DIR,database,f'{table_name}.csv')

    # Checks if table exists
    if os.path.isfile(table_path):

        # Reads table
        table_df = pd.read_csv(table_path)

        # Splits raw command on where, removes semicolon, splits on space, removes whitespace, and drops empty elements
        where_command = raw_command[raw_command.lower().find('where'):]

        # Runs if there was a where command
        if 'where' in where_command.lower():
            where_command = where_command.replace(';','').split(' ')
            where_command = [x.strip() for x in where_command]
            where_command = [x for x in where_command if x != '']
        
            filter_series = where(where_command=where_command, table_df=table_df)

            table_df.drop(table_df.loc[filter_series].index, inplace=True)

            print(f'Deleted {sum(filter_series)} records.\n')

        # If no where command entire table is deleted
        else:
            table_df.drop(table_df.index, inplace=True)
            print(f'Deleted all records.\n')

        # Saves new table
        table_df.to_csv(table_path, index = False)


    # If the table already exists raise exception
    else:
        raise Invalid_Command(f'Failed to insert. Table {table_name} could not be found.\n')
    

    # Returns database so we can continue using it
    return database


def where(where_command, table_df):
    '''
    This function returns DataFrame after applying the where condition.

    Where condition formatted as: ['WHERE','{column_name}', '{comparison operator}', '{value}'] eg. 'WHERE','a', '=', '2']

    Returns: boolean series to filter df
    '''

    operator_dict = {
        '=': operator.eq,
        '!=': operator.ne,
        '<': operator.lt,
        '<=': operator.le,
        '>': operator.gt,
        '>=': operator.ge,
    }

    if (where_command[1].lower() in table_df.columns) and (where_command[3].lower() in table_df.columns):
        col_1 = where_command[1].lower()
        comparison = where_command[2]
        comparison_function = operator_dict[comparison]
        col_2 = where_command[3].lower()

        if comparison in operator_dict:
                filter_series = comparison_function(table_df[col_1],table_df[col_2])
        else:
                raise Invalid_Command('No matching comparison operator found.')
        

    else:
        column = where_command[1].lower()
        comparison = where_command[2]
        comparison_function = operator_dict[comparison]
        value = format_values(where_command[3].lower())

        if column in table_df.columns:

            if comparison in operator_dict:
                filter_series = comparison_function(table_df[column],value)
            else:
                raise Invalid_Command('No matching comparison operator found.')

    return filter_series


def update_table(command, database, raw_command, **kwargs):
    '''
    This function updates values in a specified table using a WHERE statment.

    Returns: database
    '''

    # Checks for database and INTO keyword
    if database == '':
        raise Invalid_Command('No database specified.\n')
    
    # Gets table name and path
    table_name = command[1]
    table_path = os.path.join(DATABASE_DIR,database,f'{table_name}.csv')

    # Checks if table exists
    if os.path.isfile(table_path):

        table_df = pd.read_csv(table_path)

        # Splits raw command on where, removes semicolon, splits on space, removes whitespace, and drops empty elements
        where_command = raw_command[raw_command.lower().find('where'):]
        # Runs if there was a where command
        if 'where' in where_command.lower():
            where_command = where_command.replace(';','').split(' ')
            where_command = [x.strip() for x in where_command]
            where_command = [x for x in where_command if x != '']
        
            filter_series = where(where_command=where_command, table_df=table_df)


        # Subsets to the set statement
        set_command = raw_command[raw_command.lower().find('set'):raw_command.lower().find('where')]
        if 'set' in set_command.lower():
            # Splits on spaces, strips, and removes empty elements
            set_command = set_command.split(' ')
            set_command = [x.strip() for x in set_command]
            set_command = [x for x in set_command if x != '']

            # Selects column and formats value
            column = set_command[1].lower()
            value = format_values(set_command[3])

            if column in table_df.columns:
                
                # Sets value
                table_df.loc[filter_series,column] = value

                table_df.to_csv(table_path, index = False)

                print(f'Modified {sum(filter_series)} records.\n')

            else:
                raise Invalid_Command("Column not found.\n")

    else:
        raise Invalid_Command("Table not found.\n")

    return database


def execute_command(command, database):
    '''
    This function executes one sql command. It first checks for a semi-colon and then
    uses the first word to find the right function to execute.

    Returns: database_name
    '''

    # Creates copy of original command
    raw_command = command

    command = command.lower()

    # Checks if command includes semi-colon and raises error if not
    if ';' in command:
        command = command.replace(';','')
        # Splits command into array wtih each element being a word and removes whitespaces
        command = command.split(' ')
        command = [x.strip() for x in command]
        if '' in command:
            command = [x for x in command if x != '']
    else:
        raise Invalid_Command('No semi-colon found. Not a valid input.\n')

    # Tries to find matching command function using first word
    command_type = command[0]
    try:
        command_function = command_dict[command_type]
    #  If command is not found raises Invalid_Command error
    except KeyError:
        raise Invalid_Command(f'{command_type} is not a valid SQL command.\n')

    # Runs matching command function
    database = command_function(command=command, database=database, raw_command=raw_command)

    return database


# Dictionary to match sql command to matching function
command_dict = {
    'create': create,
    'drop': drop,
    'use': use_database,
    'select': select_command,
    'alter': alter_table,
    'insert': insert,
    'update': update_table,
    'delete': delete
}
