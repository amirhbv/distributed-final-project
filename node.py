from datetime import datetime, timedelta
from socket import AF_INET, SO_BROADCAST, SOCK_DGRAM, SOL_SOCKET, socket
from threading import Thread
from time import sleep
from typing import List, Set, Tuple

from enums import (ACK_FOR_JOIN, BROADCAST_ADDRESS, BROADCAST_LISTEN_PORT,
                   BROADCAST_TIME_LIMIT_IN_SECONDS, DATA_LIST_SPLITTER,
                   DATA_SPLITTER, DATA_TUPLE_SPLITTER, DEFUALT_ADDRESS, FILE_SEARCH_RESULT, REQUEST_FOR_FILE,
                   REQUEST_FOR_JOIN, REQUEST_FOR_NEIGHBOR, STATE_WAIT, STATE_SEARCH,
                   UDP_LISTEN_PORT)
from filesystem import FileSystem


class Packet:
    @classmethod
    def from_message(cls, data):
        data = data.decode().split(DATA_SPLITTER)
        command, *data = data
        if command == REQUEST_FOR_JOIN:
            return BroadcastPacket()
        elif command == ACK_FOR_JOIN:
            return BroadcastAckPacket(data[0])
        elif command == REQUEST_FOR_NEIGHBOR:
            return NeighborRequestPacket()
        elif command == REQUEST_FOR_FILE:
            return SearchFilePacket(data[0], data[1].split(DATA_LIST_SPLITTER) if data[1] else [])
        elif command == FILE_SEARCH_RESULT:
            return SearchResultPacket(
                data[0],
                data[1].split(DATA_LIST_SPLITTER) if data[1] else [],
                [
                    tuple(_.split(DATA_TUPLE_SPLITTER))
                    for _ in (data[2].split(DATA_LIST_SPLITTER) if data[2] else [])
                ],
            )

        return Packet('ERRRRRRROR')

    def __init__(self, data) -> None:
        self.data = data

    def encode(self):
        return DATA_SPLITTER.join([str(_) for _ in self.data]).encode()


class BroadcastPacket(Packet):
    def __init__(self) -> None:
        return super().__init__(data=[REQUEST_FOR_JOIN])


class BroadcastAckPacket(Packet):
    def __init__(self, number_of_neighbors) -> None:
        self.number_of_neighbors = int(number_of_neighbors)
        return super().__init__(data=[ACK_FOR_JOIN, number_of_neighbors])


class NeighborRequestPacket(Packet):
    def __init__(self) -> None:
        return super().__init__(data=[REQUEST_FOR_NEIGHBOR])


class SearchFilePacket(Packet):
    def __init__(self, file_name, reached_nodes) -> None:
        self.file_name = file_name
        self.reached_nodes = reached_nodes
        return super().__init__(data=[REQUEST_FOR_FILE, file_name, DATA_LIST_SPLITTER.join(reached_nodes)])


class SearchResultPacket(Packet):
    def __init__(self, file_name, reached_nodes, files) -> None:
        self.file_name = file_name
        self.reached_nodes = reached_nodes
        self.files = files
        return super().__init__(data=[
            FILE_SEARCH_RESULT,
            file_name,
            DATA_LIST_SPLITTER.join(reached_nodes),
            DATA_LIST_SPLITTER.join(
                [DATA_TUPLE_SPLITTER.join(_) for _ in files]
            ),
        ])


class Node:
    def __init__(self, directory):
        self.neighbors: Set[str] = set()
        self.file_system = FileSystem(directory)

    def run(self):
        self.send_socket = socket(AF_INET, SOCK_DGRAM)
        self.send_socket.connect(("8.8.8.8", 80))
        self.ip_address = self.send_socket.getsockname()[0]

        Thread(target=self.handle_broadcast).start()
        Thread(target=self.handle_udp_message).start()

        self.broadcast()
        self.choose_neighbors()
        print('end', self.neighbors)

        self.run_user_interface()

    def handle_incoming_message(self, sock):
        while True:
            msg, address = sock.recvfrom(1024)
            address = address[0]
            if address == self.ip_address:
                continue

            self.handle_packet(
                packet=Packet.from_message(msg),
                from_address=address,
            )

    def handle_broadcast(self):
        broadcast_socket = socket(AF_INET, SOCK_DGRAM)
        broadcast_socket.bind((DEFUALT_ADDRESS, BROADCAST_LISTEN_PORT))
        self.handle_incoming_message(broadcast_socket)

    def handle_udp_message(self):
        udp_socket = socket(AF_INET, SOCK_DGRAM)
        udp_socket.bind((self.ip_address, UDP_LISTEN_PORT))
        self.handle_incoming_message(udp_socket)

    def handle_packet(self, packet: Packet, from_address: tuple):
        print(packet, packet.encode(), from_address)

        if isinstance(packet, BroadcastPacket):
            self.handle_broadcast_packet(from_address)
        elif isinstance(packet, BroadcastAckPacket):
            self.handle_broadcast_ack_packet(packet, from_address)
        elif isinstance(packet, NeighborRequestPacket):
            self.handle_neighbor_request_packet(from_address)
        elif isinstance(packet, SearchFilePacket):
            self.handle_search_file_packet(packet, from_address)
        elif isinstance(packet, SearchResultPacket):
            self.handle_search_result_packet(packet)

    def handle_broadcast_packet(self, from_address):
        self.send_socket.sendto(
            BroadcastAckPacket(len(self.neighbors)).encode(),
            (from_address, UDP_LISTEN_PORT),
        )

    def handle_broadcast_ack_packet(self, packet: BroadcastAckPacket, from_address):
        self.potential_neighbors[from_address] = packet.number_of_neighbors

    def handle_neighbor_request_packet(self, from_address):
        self.add_neighbor(from_address)

    def handle_search_file_packet(self, packet: SearchFilePacket, from_address):
        has_sent_packet = self.handle_search(
            file_name=packet.file_name,
            current_reached_nodes=packet.reached_nodes,
        )
        if not has_sent_packet:
            self.handle_file_search_result(
                file_name=packet.file_name,
                reached_nodes=packet.reached_nodes,
            )

    def handle_search_result_packet(self, packet: SearchResultPacket):
        self.handle_file_search_result(
            file_name=packet.file_name,
            reached_nodes=packet.reached_nodes,
            current_files=packet.files,
        )

    def broadcast(self):
        self.potential_neighbors = dict()

        broadast_socket = socket(AF_INET, SOCK_DGRAM)
        broadast_socket.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
        start_time = datetime.now()
        while datetime.now() - start_time < timedelta(seconds=BROADCAST_TIME_LIMIT_IN_SECONDS):
            broadast_socket.sendto(
                BroadcastPacket().encode(),
                (BROADCAST_ADDRESS, BROADCAST_LISTEN_PORT),
            )
            sleep(0.5)

    def choose_neighbors(self):
        if not self.potential_neighbors:
            return

        sorted_potential_neighbors = sorted([
            (number_of_neighbors, address)
            for address, number_of_neighbors in self.potential_neighbors.items()
        ])
        number_of_neighbors = sorted_potential_neighbors[-1][0] or 1
        print(number_of_neighbors, sorted_potential_neighbors)
        for i in range(number_of_neighbors):
            address = sorted_potential_neighbors[i][1]
            self.add_neighbor(address)
            self.send_socket.sendto(
                NeighborRequestPacket().encode(),
                (address, UDP_LISTEN_PORT),
            )

    def add_neighbor(self, neighbor_address):
        self.neighbors.add(neighbor_address)
        print(neighbor_address, self.neighbors)

    def run_user_interface(self):
        self.state = STATE_SEARCH
        while True:
            if self.state == STATE_SEARCH:
                file_name = input("Enter file name:\n")
                self.handle_search(file_name)
                self.state = STATE_WAIT
            elif self.state == STATE_WAIT:
                pass
            else:
                file_index = input("Choose file from the list (0 for cancel)")
                if file_index == '0':
                    self.state = STATE_SEARCH
                    continue
                else:
                    # self.download_file(file_index)
                    pass

    def handle_search(self, file_name, current_reached_nodes=[]):
        print("search", current_reached_nodes)
        has_sent_packet = False
        for neighbor in self.neighbors:
            if neighbor not in current_reached_nodes:
                self.send_socket.sendto(
                    SearchFilePacket(
                        file_name=file_name,
                        reached_nodes=[
                            self.ip_address,
                            *current_reached_nodes,
                        ],
                    ).encode(),
                    (neighbor, UDP_LISTEN_PORT),
                )
                has_sent_packet = True
        return has_sent_packet

    def handle_file_search_result(self, file_name, reached_nodes, current_files: List[Tuple] = []):
        print("search result", reached_nodes)
        if reached_nodes:
            files = self.file_system.search_for_file(file_name)
            print('found: ', files)
            destination, *reached_nodes = reached_nodes
            self.send_socket.sendto(
                SearchResultPacket(
                    file_name=file_name,
                    reached_nodes=reached_nodes,
                    files=[
                        *[(file, self.ip_address) for file in files],
                        *current_files,
                    ],
                ).encode(),
                (destination, UDP_LISTEN_PORT),
            )
        else:
            print(current_files)
