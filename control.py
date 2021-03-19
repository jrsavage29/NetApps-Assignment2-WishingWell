#!/usr/bin/env python3
import sys
import time
import os
import pika
import pymongo


def callback(ch, method, properties, body):
    print(f"[Ctrl 07] - Consumed message {body.decode()} on {method.exchange}:{method.routing_key}")

def main(argv):
    repo_ip = None  # 127.0.0.1
    repo_port = None  # 5672

    user_input = None
    # exchange name is also known as the place name (e.g. Squires, Library, Goodwin)
    exchange_name = None
    # queue name is also known as the subject name (e.g. Rooms, Wishes, Classrooms)
    queue_name = None
    # message is just the message that should be produced (if a message is in the user's command then it will be a produce command)
    message = None

    # The user and password for admin connection to virtual host
    username = "assignment2"
    password = "wishingwell"

    # possible boolean flag to use for notifying consumuing or producing command has been entered
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
        # parsing the arguments and putting them into their respective variables

    for argpos in range(len(argv)):
        if argv[argpos] == '-rip':
            repo_ip = argv[argpos + 1]
        elif argv[argpos] == '-rport':
            repo_port = argv[argpos + 1]

    # Display what was parsed
    print("[Ctrl 01] - Connecting to RabbitMQ instance on" + str(repo_ip) + " with port " + str(repo_port))

    # setting up connection params for RabbitMQ
    credentials = pika.PlainCredentials(username, password)
    parameters = pika.ConnectionParameters(repo_ip, repo_port, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # Initialize exchanges and queues
    print("[Ctrl 02] - Initialized Exchanges and Queues: ")
    exchanges = ["Squires", "Goodwin", "Library"]
    squires_queues = ["Food", "Meetings", "Rooms"]
    goodwin_queues = ["Classrooms", "Auditorium"]
    library_queues = ["Noise", "Seating", "Wishes"]

    for exchange in exchanges:
        if exchange == "Squires":
            for queue in squires_queues:
                channel.exchange_declare(exchange=exchange, exchange_type='direct')
                channel.queue_declare(queue=queue)
                print(f'{exchange}:{queue}')
                channel.queue_bind(exchange=exchange, queue=queue, routing_key=queue)

        elif exchange == "Goodwin":
            for queue in goodwin_queues:
                channel.exchange_declare(exchange=exchange, exchange_type='direct')
                channel.queue_declare(queue=queue)
                print(f"{exchange}:{queue}")
                channel.queue_bind(exchange=exchange, queue=queue, routing_key=queue)

        elif exchange == "Library":
            for queue in library_queues:
                channel.exchange_declare(exchange=exchange, exchange_type='direct')
                channel.queue_declare(queue=queue)
                print(f'{exchange}:{queue}')
                channel.queue_bind(exchange=exchange, queue=queue, routing_key=queue)

    # Initialize MongoDB database
    squiresDB = pymongo.MongoClient().Squires
    goodwinDB = pymongo.MongoClient().Goodwin
    libraryDB = pymongo.MongoClient().Library
    print("[Ctrl 03] - Initialized MongoDB datastore")

    # parse the user command and determine if it's a produce or consume command
    while True:
        user_input = str(input("[Ctrl 04] - Enter your command: "))
        action = ""
        # storing original input just in case
        original_input = user_input

        if user_input == "exit()":
            print("[Ctrl 08] - Exiting")
            sys.exit(0)

        try:
            # parsing the input
            assert 'p:' in user_input or 'c:' in user_input
            # if there's a message then we know this a producing command
            if 'p:' in user_input:
                print('This is a producing command')
                is_producing = True
                action = action.replace("", "p")
                # parsing input for the exchange name then striping it from the input string along with other useless chars
                start_index = user_input.find(':') + 1
                end_index = user_input.find('+')

                exchange_name = (user_input[start_index: end_index]).strip()
                user_input = user_input.strip('p:')
                user_input = user_input.strip(exchange_name)

                # parsing input for the queue name then striping it from the previously parsed input string along with other useless chars
                start_index = user_input.find('+') + 1
                end_index = user_input.find(" ")

                queue_name = (user_input[start_index:end_index]).strip()
                user_input = user_input.strip('+')
                user_input = user_input.strip(queue_name)

                # The remainder of the parsed input string should just be the message
                message = user_input.strip()

            elif 'c:' in user_input:
                print('This is a consuming command')
                is_producing = False
                action = action.replace("", "c")
                # parsing input for the exchange name then striping it from the input string along with other useless chars
                start_index = user_input.find(':') + 1
                end_index = user_input.find('+')

                exchange_name = (user_input[start_index:end_index]).strip()

                user_input = user_input.strip('c:')
                start_index = user_input.find('+') + 1

                # The remainder of the parsed input string should just be the queue name
                queue_name = (user_input[start_index:]).strip()
            # validate you received the correct input for a possible exchange according to the project spec
            assert exchange_name == 'Squires' or exchange_name == 'Goodwin' or exchange_name == 'Library', exchange_name + " is not a valid exchange!"

            # Validate you received a valid input for the queue name according to the possible names listed in the specs
            if exchange_name == 'Squires':
                assert queue_name == 'Food' or queue_name == 'Meetings' or queue_name == 'Rooms', queue_name + " is not a valid queue for Squires!"

            elif exchange_name == 'Goodwin':
                assert queue_name == 'Classrooms' or queue_name == 'Auditorium', queue_name + " is not a valid queue for Goodwin!"

            elif exchange_name == 'Library':
                assert queue_name == 'Noise' or queue_name == 'Seating' or queue_name == 'Wishes', queue_name + " is not a valid queue for Library!"

            msg_id = "15" + "$" + str(time.time())
            db_stats = {"Action": action,
                        "Place": exchange_name,
                        "MsgID": msg_id,
                        "Subject": queue_name,
                        "Message": message
                        }

            print(f"[Ctrl 05] – Inserted command into MongoDB: {db_stats}")

            if exchange_name == 'Squires':
                if queue_name == 'Food':
                    squiresDB.Food.insert_one(db_stats)
                elif queue_name == 'Meetings':
                    squiresDB.Meetings.insert_one(db_stats)
                elif queue_name == 'Rooms':
                    squiresDB.Rooms.insert_one(db_stats)
            elif exchange_name == 'Goodwin':
                if queue_name == 'Classrooms':
                    goodwinDB.Classrooms.insert_one(db_stats)
                elif queue_name == 'Auditorium':
                    goodwinDB.Auditorium.insert_one(db_stats)
            elif exchange_name == 'Library':
                if queue_name == 'Noise':
                    libraryDB.Noise.insert_one(db_stats)
                elif queue_name == 'Seating':
                    libraryDB.Seating.insert_one(db_stats)
                elif queue_name == 'Wishes':
                    libraryDB.Wishes.insert_one(db_stats)

            # print statement to verify that everything was parsed correctly
            if is_producing:
                print(f"[Ctrl 06] - Produced message {message} on {exchange_name}:{queue_name}")
                byteMsg = message.encode()
                channel.basic_publish(exchange=exchange_name, routing_key=queue_name, body=byteMsg)
            else:
                # place a variable containing the consumed message in the empty bracket
                channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
                try:
                    channel.start_consuming()
                except KeyboardInterrupt:
                    channel.stop_consuming()

        except AssertionError as msg:
            print(msg, "Please enter a valid command. If you're trying to exit, use the command \"exit()\"")



if __name__ == "__main__":
    main(sys.argv[1:])
