#!/usr/bin/env python3
import sys
import os
import json
import pika
import pymongo

def main(argv):
    repo_ip = None #127.0.0.1
    repo_port = None #50000

    user_input = None
    #exchange name is also known as the place name (e.g. Squires, Library, Goodwin)
    exchange_name = None
    #queue name is also known as the subject name (e.g. Rooms, Wishes, Classrooms)
    queue_name = None
    #message is just the message that should be produced (if a message is in the user's command then it will be a produce command)
    message = None

    #possible boolean flag to use for notifying consumuing or producing command has been entered
    is_producing = False

    # Take the list of arguments from the command line and parse them
    if len(argv) != 4:
        # if we are missing arguments then we get an error output the correct usage
        print("usage: control.py -rip <REPOSITORY_IP> -rport <REPOSITORY_PORT> ")
        sys.exit(1)
    if "-rip" not in argv or "-rport" not in argv:
        # if we are missing arguments then we get an error output the correct usage
        print("usage: control.py -rip <REPOSITORY_IP> -rport <REPOSITORY_PORT> ")
        sys.exit(1)
        # parsing the arguements and putting them into their respective variables

    for argpos in range(len(argv)):
        if argv[argpos] == '-rip':
            repo_ip = argv[argpos + 1]
        elif argv[argpos] == '-rport':
            repo_port = argv[argpos + 1]
        
    # Display what was parsed
    print("[] - Connecting to " + str(repo_ip) + " on port " + str(repo_port))

    #parse the user command and determine if it's a produce or consume command
    while True:
        user_input = str(input("Enter your command: "))
        
        #storing original input just in case
        original_input = user_input

        try:
            #parsing the input
            assert 'p:' in user_input or 'c:' in user_input, "That's an invalid command. Please try again." 

            #if there's a message then we know this a producing command
            if 'p:' in user_input: 
                print('This is a producing command')
                is_producing = True
                
                #parsing input for the exchange name then striping it from the input string along with other useless chars
                start_index = user_input.find(':') + 1
                end_index =  user_input.find('+')

                exchange_name = (user_input[start_index : end_index]).strip()
                user_input = user_input.strip('p:')
                user_input = user_input.strip(exchange_name)

                #parsing input for the queue name then striping it from the previously parsed input string along with other useless chars
                start_index = user_input.find('+') + 1
                end_index = user_input.find(" ")

                queue_name = (user_input[start_index:end_index]).strip()
                user_input = user_input.strip('+')
                user_input = user_input.strip(queue_name)
                
                #The remainder of the parsed input string should just be the message
                message = user_input.strip()


            elif 'c:' in user_input:
                print('This is a consuming command')
                is_producing = False

                #parsing input for the exchange name then striping it from the input string along with other useless chars
                start_index = user_input.find(':') + 1
                end_index =  user_input.find('+')

                exchange_name = (user_input[start_index:end_index]).strip()
                
                user_input = user_input.strip('c:')
                user_input = user_input.strip(exchange_name)
                user_input = user_input.strip('+')
                
                #The remainder of the parsed input string should just be the queue name
                queue_name = user_input.strip()


            #validate you recieved the correct input for a possible exchange according to the project spec
            assert exchange_name == 'Squires' or exchange_name == 'Goodwin' or exchange_name == 'Library', exchange_name + " is not a valid exchange!"

            #Validate you recieved a valid input for the queue name according to the possible names listed in the specs
            if exchange_name == 'Squires':
                 assert queue_name == 'Food' or queue_name == 'Meetings' or queue_name == 'Rooms', queue_name + " is not a valid queue for Squires!"
            
            elif exchange_name == 'Goodwin':
                 assert queue_name == 'Classrooms' or queue_name == 'Auditorium', queue_name + " is not a valid queue for Goodwin!"
            
            elif exchange_name == 'Library':
                 assert queue_name == 'Noise' or queue_name == 'Seating' or queue_name == 'Wishes', queue_name + " is not a valid queue for Library!"
        
        except AssertionError as msg:
            print(msg, "Please enter a valid command.")

        else:
            break
    
    #simple print statement to verify that everything was parsed correctly
    if is_producing == True:
        print("The Place is:|", exchange_name, "|\nThe Subject is:|", queue_name, "|\nThe Message is:|", message,"|")
    else:
        print("The Place is:|", exchange_name, "|\nThe Subject is:|", queue_name,"|")


    

if __name__ == "__main__":
    main(sys.argv[1:])