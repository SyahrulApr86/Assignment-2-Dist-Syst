import logging
from math import log
import random
import threading
import time
import socket
from ast import literal_eval
from pprint import pformat

def start_fault_timer(key):
    status_dictionary[key][1] = False
    logger.info(f"This node become a fault: {key}")
    logger.info(f"Node fault status_dictionary:\n{pformat(status_dictionary)}")

def send_message(node_id, port):
    logger.debug("Create the client socket")
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # Note that this socket is UDP
    logger.debug("Encode the message")
    message = f"node-{node_id}#{status_dictionary}"
    logger.debug(f"message: {message}")
    message = message.encode("UTF-8")
    addr = ("127.0.0.1", port)
    logger.debug("Send the message")
    client_socket.sendto(message, addr)

def sending_procedure(heartbeat_duration, node_id, neighbors_port, node_ports, main_port, num_of_neighbors_to_choose):
    # TODO
    # Create a socket to send the heartbeat to the neighbors and main node
    # Arguments: Heartbeat duration, this Node ID, Neighbors port, Node ports, Main Node port
    # Note: use send_message function to send the heartbeat
    ""
    logger.info("Sending Procedure Thread Created")
    while True:
        time.sleep(heartbeat_duration)
        logger.info(f"Increase heartbeat node-{node_id}:")
        status_dictionary[f"node-{node_id}"][0] += 1
        status_dictionary[f"node-{node_id}"][1] = True
        logger.info(pformat(status_dictionary))
        logger.info("Determining which node to send...")
        selected_ports = random.sample(neighbors_port, min(len(neighbors_port), num_of_neighbors_to_choose))
        logger.info(f"Send messages to main node, {', '.join([f'node-{node_ports[port]}' for port in selected_ports])}")
        for port in selected_ports:
            send_message(node_id, port)
        send_message(node_id, main_port)

        
def tcp_listening_procedure(port, node_id):
    # TODO
    # Create a TCP socket to listen to the main node
    # Arguments: Main Node port, Node ID
    ""
    logger.info("Initiating TCP socket")
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(("127.0.0.1", port))
    server_socket.listen(5)
    while True:
        client_socket, addr = server_socket.accept()
        with client_socket:
            message = f"node-{node_id}#{status_dictionary}".encode("UTF-8")
            client_socket.send(message)


def listening_procedure(port, node_id, fault_duration):
    fault_timers = {}

    # Inisialisasi timer kegagalan untuk semua node kecuali diri sendiri
    for key in status_dictionary:
        if key != f'node-{node_id}':
            timer = threading.Timer(fault_duration, start_fault_timer, [key])
            timer.start()
            fault_timers[key] = timer

    logger.info("Start the timer for fault duration...")

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp_server_socket:
        udp_server_socket.bind(("127.0.0.1", port))
        logger.info("UDP server socket bound and listening.")

        while True:
            try:
                data, addr = udp_server_socket.recvfrom(1024)
                process_received_data(data, node_id, fault_duration, fault_timers)
            except socket.timeout:
                logger.info("Socket receive timed out.")


def process_received_data(data, node_id, fault_duration, fault_timers):
    sender_node_id, sender_status_dict = parse_received_data(data)
    logger.info(f"Data received from node-{sender_node_id}. Processing...")

    for key, value in sender_status_dict.items():
        compare_and_update_status(key, value, node_id, fault_duration, fault_timers)


def parse_received_data(data):
    decoded_data = data.decode("UTF-8")
    sender_node_id, status_dict_str = decoded_data.split("#", 1)
    status_dict = literal_eval(status_dict_str)
    return int(sender_node_id.split("-")[1]), status_dict


def compare_and_update_status(key, value, node_id, fault_duration, fault_timers):
    current_status = status_dictionary.get(key, [0, False])
    incoming_heartbeat, is_alive = value

    # Logika pembaruan status
    if need_to_update_status(current_status, value):
        status_dictionary[key] = value
        if need_to_restart_fault_timer(is_alive, current_status, value):
            restart_fault_timer(key, fault_duration, fault_timers)

    logger.info(f"Status updated: {status_dictionary}")


def need_to_update_status(current_status, incoming_status):
    return incoming_status[0] > current_status[0] or (
                incoming_status[1] != current_status[1] and incoming_status[0] >= current_status[0])


def need_to_restart_fault_timer(is_alive, current_status, incoming_status):
    return is_alive or current_status[1] != incoming_status[1]


def restart_fault_timer(key, fault_duration, fault_timers):
    if key in fault_timers:
        fault_timers[key].cancel()
    fault_timers[key] = threading.Timer(fault_duration, start_fault_timer, [key])
    fault_timers[key].start()
    logger.info(f"Fault timer restarted for {key}.")


def handle_exception(exc_type, exc_value, exc_traceback):
    logger.error(f"Uncaught exception handler", exc_info=(exc_type, exc_value, exc_traceback))

def thread_exception_handler(args):
    logger.error(f"Uncaught exception", exc_info=(args.exc_type, args.exc_value, args.exc_traceback))

def reload_logging_windows(filename):
    log = logging.getLogger()
    for handler in log.handlers:
        log.removeHandler(handler)
    logging.basicConfig(format='%(asctime)-4s %(levelname)-6s %(threadName)s:%(lineno)-3d %(message)s',
                        datefmt='%H:%M:%S',
                        filename=filename,
                        filemode='w',
                        level=logging.DEBUG)

def main(heartbeat_duration=1, num_of_neighbors_to_choose=1,
         fault_duration=1, port=1000, node_id=1, neighbors_ports=[1000,1001], main_port=999):
    reload_logging_windows(f"logs/node{node_id}.txt")
    global logger
    logger = logging.getLogger(__name__)
    try:
        logger.info(f"Node with id {node_id} is running")
        logger.debug(f"heartbeat_duration: {heartbeat_duration}")
        logger.debug(f"fault_duration: {fault_duration}")
        logger.debug(f"port: {port}")
        logger.debug(f"num_of_neighbors_to_choose: {num_of_neighbors_to_choose}")
        logger.debug(f"initial neighbors_ports: {neighbors_ports}")

        logger.info("Configure the status_dictionary global variable")
        global status_dictionary
        status_dictionary = {}
        node_ports = {}
        for i in range(len(neighbors_ports)):
            status_dictionary[f"node-{i + 1}"] = [0, True]
            node_ports[neighbors_ports[i]] = i+1
        neighbors_ports.remove(port)
        logger.debug(f"final neighbors_ports: {neighbors_ports}")
        logger.info(f"status_dictionary:\n{pformat(status_dictionary)}")
        logger.info("Done configuring the status_dictionary")

        logger.info("Executing the status check procedure")
        thread = threading.Thread(target=tcp_listening_procedure, args=(port, node_id))
        thread.name = "tcp_listening_thread"
        thread.start()

        logger.info("Executing the listening procedure")
        threading.excepthook = thread_exception_handler
        thread = threading.Thread(target=listening_procedure, args=(port, node_id, fault_duration))
        thread.name = "listening_thread"
        thread.start()
        
        logger.info("Executing the sending procedure")
        thread = threading.Thread(target=sending_procedure,
                         args=(heartbeat_duration,
                               node_id, neighbors_ports, node_ports, main_port, num_of_neighbors_to_choose))
        thread.name = "sending_thread"
        thread.start()
    except Exception as e:
        logger.exception("Caught Error")
        raise

if __name__ == '__main__':
    main()
