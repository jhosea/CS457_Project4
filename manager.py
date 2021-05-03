'''
Project 1 - CS 457
Simple Database and Queries
Created By Joshua Hosea at UNR
'''

import sys

import sql_commands as sql

def run_input_file(input_file):
    '''
    This function reads an input file and splits the commands on new line characters.
    It will loop through all commands and execute them in order.

    Returns: 'All commands executed.'
    '''
    with open(sys.argv[1],'r') as f:
        sql_file = f.read()

    # Replaces new lines with spaces and adds a new line after semicolon, then splits using new line character
    sql_array = sql_file.replace('\n',' ')
    sql_array = sql_array.replace(';',';\n')
    sql_array = sql_array.split('\n')
    # Removes leading and trailing white space
    sql_array = [x.strip() for x in sql_array]
    sql_array = [x for x in sql_array if x != '']

    database = ''

    for command in sql_array:
        print(f'Command entered: {command}')

        try:
            database = sql.execute_command(command, database)

        # Prints exeption if command isn't exit and continues
        except sql.Invalid_Command as ex:
            print(ex)

    print('All commands executed.')

    return None


def run_standard_input():
    '''
    This function reads one line for the input command and executes the command.

    Returns: None
    '''
    command = ''
    database = ''

    while(command != 'exit'):

        # Continues accepting input until a semicolon is inputted or the input is exit
        command = ''
        while(';' not in command and command != 'exit'):
            new_line = input('--> ')
            command += ' ' + new_line
            command = command.strip()

        try:
            database = sql.execute_command(command, database)

        # Prints exeption if command isn't exit and continues
        except sql.Invalid_Command as ex:
            if command != 'exit':
                print(ex)

    return None


if __name__ == '__main__':

    # If a file was specified in the command line, we will run the program in file mode
    if len(sys.argv) > 1:
        run_input_file(sys.argv)

    # Else use standard input
    else:
        run_standard_input()
