from datetime import datetime, timedelta
from socket import AF_INET, SO_BROADCAST, SOCK_DGRAM, SOL_SOCKET, socket
from threading import Thread
from time import sleep
from typing import Set

from enums import (ACK_FOR_JOIN, BROADCAST_ADDRESS, BROADCAST_LISTEN_PORT,
                   BROADCAST_TIME_LIMIT_IN_SECONDS, DATA_SPLITTER,
                   DEFUALT_ADDRESS, REQUEST_FOR_JOIN, REQUEST_FOR_NEIGHBOR,
                   UDP_LISTEN_PORT)


class Packet:
    @classmethod
    def from_message(cls, data):
        data = data.decode().split(DATA_SPLITTER)
        command, *data = data
        if command == REQUEST_FOR_JOIN:
            return BroadcastPacket(*data)
        elif command == ACK_FOR_JOIN:
            return BroadcastAckPacket(*data)
        elif command == REQUEST_FOR_NEIGHBOR:
            return NeighborRequestPacket(*data)

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
        self.number_of_neighbors = number_of_neighbors
        return super().__init__(data=[ACK_FOR_JOIN, number_of_neighbors])


class NeighborRequestPacket(Packet):
    def __init__(self) -> None:
        return super().__init__(data=[REQUEST_FOR_NEIGHBOR])


class Node:
    def __init__(self):
        self.neighbors: Set[str] = set()

    def run(self):
        self.send_socket = socket(AF_INET, SOCK_DGRAM)
        self.send_socket.connect(("8.8.8.8", 80))
        self.ip_address = self.send_socket.getsockname()[0]

        Thread(target=self.handle_broadcast).start()
        Thread(target=self.handle_udp_message).start()

        self.broadcast()
        self.choose_neighbors()

        print(self.neighbors)

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

    def handle_broadcast_packet(self, from_address):
        self.send_socket.sendto(
            BroadcastAckPacket(len(self.neighbors)).encode(),
            (from_address, UDP_LISTEN_PORT),
        )

    def handle_broadcast_ack_packet(self, packet: BroadcastAckPacket, from_address):
        self.potential_neighbors[from_address] = packet.number_of_neighbors

    def handle_neighbor_request_packet(self, from_address):
        self.add_neighbor(from_address)

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
            for address, number_of_neighbors in self.potential_neighbors
        ])
        number_of_neighbors = sorted_potential_neighbors[-1][0] or 1
        for i in range(number_of_neighbors):
            address = sorted_potential_neighbors[i][1]
            self.add_neighbor(address)
            self.send_socket.sendto(
                NeighborRequestPacket().encode(),
                (address, UDP_LISTEN_PORT),
            )

    def add_neighbor(self, neighbor_address):
        self.neighbors.add(neighbor_address)
