from copy import deepcopy
from datetime import datetime, timedelta
from socket import (AF_INET, SO_BROADCAST, SOCK_DGRAM, SOCK_STREAM, SOL_SOCKET,
                    socket)
from threading import Thread
from time import sleep
from typing import Iterable, List, Set
from uuid import uuid4

from enums import (ACK_FOR_JOIN, BROADCAST_ADDRESS, BROADCAST_LISTEN_PORT,
                   BROADCAST_TIME_LIMIT_IN_SECONDS, CHUNK_SIZE,
                   DATA_LIST_SPLITTER, DATA_SPLITTER, DEFUALT_ADDRESS,
                   DOWNLOAD_FILE, DOWNLOAD_FILE_REQUEST, END_CHUNK_DATA,
                   END_CHUNK_NO, FILE_SEARCH_RESULT, NEXT_PACKET_SIZE_LEN, REQUEST_FOR_FILE,
                   REQUEST_FOR_JOIN, REQUEST_FOR_NEIGHBOR, STAET_SELECT,
                   START_CHUNK_DATA, START_CHUNK_NO, STATE_SEARCH, STATE_WAIT,
                   TCP_LISTEN_PORT, UDP_LISTEN_PORT)
from file_system import FileSystem, FileSystemSearchResult
from search_tracker import FileSearchResult, SearchTracker


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
            return SearchFilePacket(data[0], data[1].split(DATA_LIST_SPLITTER) if data[1] else [], data[2])
        elif command == FILE_SEARCH_RESULT:
            return SearchResultPacket(
                data[0],
                data[1].split(DATA_LIST_SPLITTER) if data[1] else [],
                [
                    FileSearchResult.from_str(sr_str)
                    for sr_str in (data[2].split(DATA_LIST_SPLITTER) if data[2] else [])
                ],
                data[3]
            )
        elif command == DOWNLOAD_FILE_REQUEST:
            return DownloadFileRequestPacket(
                data[0],
            )
        elif command == DOWNLOAD_FILE:
            if int(data[0]) == START_CHUNK_NO:
                return DownloadFileStartPacket(
                    data[2],
                    int(data[4]),
                )
            elif int(data[0]) == END_CHUNK_NO:
                return DownloadFileEndPacket(
                    data[2],
                )
            else:
                return DownloadFilePacket(
                    int(data[0]),
                    data[1],
                    data[2],
                    data[3].split(DATA_LIST_SPLITTER) if data[3] else [],
                    int(data[4]),
                )

        return Packet('ERRRRRRROR')

    def __init__(self, data) -> None:
        self.data = data

    def encode(self):
        return self._encode(self.data)

    def _encode(self, data):
        return DATA_SPLITTER.join([str(_) for _ in data]).encode()


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
    def __init__(self, file_name, reached_nodes, search_id) -> None:
        self.file_name = file_name
        self.reached_nodes = reached_nodes
        self.search_id = search_id
        return super().__init__(data=[REQUEST_FOR_FILE, file_name, DATA_LIST_SPLITTER.join(reached_nodes), search_id])


class SearchResultPacket(Packet):
    def __init__(self, file_name, reached_nodes, search_results, search_id) -> None:
        self.file_name = file_name
        self.reached_nodes = reached_nodes
        self.search_id = search_id
        self.search_results = search_results
        return super().__init__(data=[
            FILE_SEARCH_RESULT,
            file_name,
            DATA_LIST_SPLITTER.join(reached_nodes),
            DATA_LIST_SPLITTER.join([
                str(search_result)
                for search_result in search_results
            ]),
            search_id
        ])


class DownloadFileRequestPacket(Packet):
    def __init__(self, file_name) -> None:
        self.file_name = file_name
        return super().__init__(data=[
            DOWNLOAD_FILE_REQUEST,
            file_name,
        ])


class DownloadFilePacket(Packet):
    def __init__(self, chunk_no, chunk_data, file_name, reached_nodes, next_packet_size=0) -> None:
        self.chunk_no = chunk_no
        self.chunk_data = chunk_data
        self.file_name = file_name
        self.reached_nodes = reached_nodes
        self.next_packet_size = next_packet_size
        # TODO: Escape DATA_SPLITTER character in file
        return super().__init__(data=[
            DOWNLOAD_FILE,
            chunk_no,
            chunk_data,
            file_name,
            DATA_LIST_SPLITTER.join(reached_nodes),
        ])

    def add_reached_nodes(self, new_reached_node):
        self.reached_nodes += new_reached_node
        self.set_next_packet_size(
            self.next_packet_size
            + len(new_reached_node)
            + len(DATA_LIST_SPLITTER)
        )

    def set_next_packet_size(self, next_packet_size):
        self.next_packet_size = next_packet_size

    @property
    def size(self):
        return len(self.encode())

    def _encode(self, data):
        return super()._encode(
            data=[
                *data,
                str(self.next_packet_size).rjust(NEXT_PACKET_SIZE_LEN, '0'),
            ]
        )


class DownloadFileStartPacket(DownloadFilePacket):
    def __init__(self, file_name, next_packet_size=0) -> None:
        super().__init__(START_CHUNK_NO, START_CHUNK_DATA, file_name, [], next_packet_size)


class DownloadFileEndPacket(DownloadFilePacket):
    def __init__(self, file_name) -> None:
        super().__init__(END_CHUNK_NO, END_CHUNK_DATA, file_name, [])


class Node:
    def __init__(self, directory):
        self.neighbors: Set[str] = set()
        self.file_system = FileSystem(directory)
        self.search_tracker = SearchTracker()

    def run(self):
        self.send_socket = socket(AF_INET, SOCK_DGRAM)
        self.send_socket.connect(("8.8.8.8", 80))
        self.ip_address = self.send_socket.getsockname()[0]
        Thread(target=self.handle_broadcast).start()
        Thread(target=self.handle_udp_message).start()
        Thread(target=self.handle_tcp_message).start()

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

            Thread(
                target=self.handle_packet,
                kwargs=dict(
                    packet=Packet.from_message(msg),
                    from_address=address,
                ),
            ).start()

    def handle_broadcast(self):
        broadcast_socket = socket(AF_INET, SOCK_DGRAM)
        broadcast_socket.bind((DEFUALT_ADDRESS, BROADCAST_LISTEN_PORT))
        self.handle_incoming_message(broadcast_socket)

    def handle_udp_message(self):
        udp_socket = socket(AF_INET, SOCK_DGRAM)
        udp_socket.bind((self.ip_address, UDP_LISTEN_PORT))
        self.handle_incoming_message(udp_socket)

    def handle_tcp_message(self):
        with socket(AF_INET, SOCK_STREAM) as tcp_socket:
            tcp_socket.bind((self.ip_address, TCP_LISTEN_PORT))
            tcp_socket.listen()
            conn, addr = tcp_socket.accept()
            Thread(target=self.handle_tcp_connection, args=(conn, )).start()
            print(f"Connected by {addr}")

    def handle_tcp_connection(self, conn):
        with conn:
            while True:
                data = conn.recv(1024)
                packet = Packet.from_message(data)
                if isinstance(packet, DownloadFileRequestPacket):
                    file_search_result = self.search_tracker.get_file_search_result_by_file_name(
                        file_name=packet.file_name,
                    )
                    if file_search_result.source == self.ip_address:
                        file_data = self.file_system.get_file_content(
                            packet.file_name,
                        )
                        file_size = len(file_data)
                        packets: List[DownloadFilePacket] = [
                            DownloadFileStartPacket(
                                file_name=packet.file_name,
                            )
                        ]
                        for chunk_no, chunk_start in enumerate(range(0, file_size, CHUNK_SIZE)):
                            chunk_end = min(
                                file_size,
                                chunk_start + CHUNK_SIZE,
                            )
                            chunk_data = file_data[chunk_start:chunk_end]
                            packets.append(
                                DownloadFilePacket(
                                    chunk_no=chunk_no,
                                    chunk_data=chunk_data.decode(),
                                    file_name=packet.file_name,
                                    reached_nodes=[self.ip_address],
                                )
                            )
                        packets.append(
                            DownloadFileEndPacket(
                                file_name=packet.file_name,
                            )
                        )

                        for packet_index in reversed(range(1, len(packets))):
                            packet = packets[packet_index]
                            prev_packet = packets[packet_index - 1]
                            prev_packet.set_next_packet_size(
                                packet.size
                            )
                            print(packet.encode(), packet.size)

                        for packet in packets:
                            print("SENDING:", packet.encode())
                            conn.sendall(packet.encode())

                    else:
                        for recieved_packet in self.download_file(
                            file_search_result,
                        ):
                            recieved_packet.add_reached_nodes(self.ip_address)
                            conn.sendall(recieved_packet.encode())

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
            self.handle_search_result_packet(packet, from_address)

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
            search_id=packet.search_id,
            current_reached_nodes=packet.reached_nodes,
        )
        files = self.file_system.search_for_file(packet.file_name)
        if has_sent_packet:
            self.create_search_result_response_from_neighbors(
                file_name=packet.file_name,
                search_id=packet.search_id,
                reached_nodes=packet.reached_nodes,
                files=files
            )
        else:
            self.create_search_result_response(packet, files)

    def create_search_result_response_from_neighbors(self, file_name, search_id, reached_nodes, files: List[FileSystemSearchResult]):
        while (True):
            sleep(1)
            if self.search_tracker.is_search_result_ready(search_id):
                node_search_result = self.search_tracker.create_results_from_files(
                    files,
                    self.ip_address,
                )
                search_result = self.search_tracker.get_final_search_result(
                    search_id, node_search_result)
                self.handle_file_search_result(
                    file_name, reached_nodes, search_result, search_id)
                break

    def create_search_result_response(self, packet: SearchFilePacket, files: List[FileSystemSearchResult]):
        node_search_result = self.search_tracker.create_results_from_files(
            files,
            self.ip_address,
        )
        self.search_tracker.update_file_tracker(node_search_result)
        self.handle_file_search_result(
            file_name=packet.file_name,
            reached_nodes=packet.reached_nodes,
            search_results=node_search_result,
            search_id=packet.search_id
        )

    def handle_search_result_packet(self, packet: SearchResultPacket, from_address):
        self.search_tracker.add_result_for_search(
            search_id=packet.search_id,
            result_from=from_address,
            files=packet.search_results,
        )

        # self.handle_file_search_result(
        #     file_name=packet.file_name,
        #     reached_nodes=packet.reached_nodes,
        #     current_files=packet.files,
        #     depth=packet.depth
        # )

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
            if self.state != STATE_WAIT:
                print('state', self.state)

            if self.state == STATE_SEARCH:
                self.state = STATE_WAIT
                requested_file_name = input("Enter file name:\n")
                search_id = str(uuid4().bytes)
                self.handle_search(
                    file_name=requested_file_name,
                    search_id=search_id,
                )
                self.create_search_result_response_from_neighbors(
                    file_name=requested_file_name,
                    search_id=search_id,
                    reached_nodes=[],
                    files=[]
                )
            elif self.state == STATE_WAIT:
                pass
            else:
                input_str = 'Choose file from the list (0 for cancel):\n'
                for i, _ in enumerate(self.search_results):
                    input_str += f'{str(i + 1)}: {str(_)}\n'
                file_index = input(input_str)
                if file_index == '0':
                    self.state = STATE_SEARCH
                    continue
                else:
                    file_search_result: FileSearchResult = self.search_results[int(
                        file_index) - 1
                    ]
                    packets = list(self.download_file(
                        file_search_result,
                    ))
                    self.file_system.add_new_file(
                        file_name=file_search_result.file_name,
                        file_content=b''.join(
                            [
                                _.chunk_data.encode()
                                for _ in sorted(packets, key=lambda _: _.chunk_no)
                                if _.chunk_no not in (START_CHUNK_NO, END_CHUNK_NO)
                            ]
                        ),
                    )

    def download_file(self, file_search_result: FileSearchResult) -> Iterable[DownloadFilePacket]:
        print(file_search_result)
        with socket(AF_INET, SOCK_STREAM) as download_socket:
            download_socket.connect(
                (file_search_result.source, TCP_LISTEN_PORT),
            )
            download_socket.sendall(
                DownloadFileRequestPacket(
                    file_name=file_search_result.file_name,
                ).encode()
            )
            recv_size = DownloadFileStartPacket(
                file_name=file_search_result.file_name,
            ).size
            while True:
                data = download_socket.recv(recv_size)
                print('download: ', data)
                packet = Packet.from_message(data)
                yield packet
                recv_size = packet.next_packet_size
                if isinstance(packet, DownloadFileEndPacket):
                    break

    def handle_search(self, file_name, search_id, current_reached_nodes=[]):
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
                        search_id=search_id
                    ).encode(),
                    (neighbor, UDP_LISTEN_PORT),
                )
                self.search_tracker.add_nieghbor_for_search(
                    search_id,
                    neighbor,
                )
                has_sent_packet = True
        return has_sent_packet

    def handle_file_search_result(self, file_name, reached_nodes, search_results, search_id):
        print("search result", reached_nodes)
        if reached_nodes:
            # files = self.file_system.search_for_file(file_name)
            print('found: ', search_results)
            search_results = deepcopy(search_results)
            destination, *reached_nodes = reached_nodes
            for search_result in search_results:
                search_result.source = self.ip_address
            self.send_socket.sendto(
                SearchResultPacket(
                    file_name=file_name,
                    reached_nodes=reached_nodes,
                    search_results=search_results,
                    search_id=search_id
                ).encode(),
                (destination, UDP_LISTEN_PORT),
            )
        else:
            print([str(sr)for sr in search_results])
            self.state = STAET_SELECT
            self.search_results = search_results
