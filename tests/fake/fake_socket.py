import asyncio
import struct

from luxtronik import Parameters, Calculations, Visibilities


def fake_parameter_value(i):
    return (5 * i**2 + 4 * i - 2) % 1001


def fake_calculation_value(i):
    return (23 * i**2 + 42 * i - 47) % 1001


def fake_visibility_value(i):
    return (90 * i**2 + 19 * i - 1) % 2


class FakeSocket:
    """
    Fake persistent-connection state, shared by a FakeStreamReader/FakeStreamWriter
    pair, standing in for the (reader, writer) tuple `asyncio.open_connection()`
    would normally return.
    """

    last_instance = None
    prev_instance = None
    open_connection_exception = None
    force_recv_result = None

    # These code are hard coded here in order to prevent
    # accidential changes in constants.py
    code_write_parameter = 3002
    code_read_parameters = 3003
    code_read_calculations = 3004
    code_read_visibilities = 3005

    def __init__(self):
        FakeSocket.prev_instance = FakeSocket.last_instance
        FakeSocket.last_instance = self

        self._closing = False
        self._buffer = b""

        # Offer some more entries
        self._num_paras = len(Parameters()._data) + 10
        self._num_calcs = len(Calculations()._data) + 10
        self._num_visis = len(Visibilities()._data) + 10

        self.written_values = {}

    def close(self):
        self._closing = True

    def is_closing(self):
        return self._closing

    def write(self, data):
        assert not self._closing

        cnt = len(data) // 4
        content = struct.unpack(">" + "i" * cnt, data)

        # Next, we compute our response, which is saved in self._buffer.
        # The client can read the response with self.readexactly()

        if content[0] == FakeSocket.code_write_parameter:
            # Client wants to write a parameters
            assert cnt == 3
            idx = content[1]
            value = content[2]

            # Remember the written values, so we can test them later on
            self.written_values[idx] = value

            # Respond with code and idx of parameter
            self._buffer += struct.pack(">i", FakeSocket.code_write_parameter)
            self._buffer += struct.pack(">i", idx)

        elif content[0] == FakeSocket.code_read_parameters:
            # Client wants to read parameters
            assert cnt == 2

            # Respond with code...
            self._buffer += struct.pack(">i", FakeSocket.code_read_parameters)

            # ... length of the response...
            response_cnt = self._num_paras
            self._buffer += struct.pack(">i", response_cnt)

            # ... and the payload
            for i in range(response_cnt):
                self._buffer += struct.pack(">i", fake_parameter_value(i))

        elif content[0] == FakeSocket.code_read_calculations:
            # Client wants to read calculations
            assert cnt == 2

            # Respond with code...
            self._buffer += struct.pack(">i", FakeSocket.code_read_calculations)

            # ...for calculations, we need a 'status' response...
            self._buffer += struct.pack(">i", 0)

            # ... length of the response...
            response_cnt = self._num_calcs
            self._buffer += struct.pack(">i", response_cnt)

            # ... and the payload
            for i in range(response_cnt):
                self._buffer += struct.pack(">i", fake_calculation_value(i))

        elif content[0] == FakeSocket.code_read_visibilities:
            # Client wants to read visibilities
            assert cnt == 2

            # Respond with code...
            self._buffer += struct.pack(">i", FakeSocket.code_read_visibilities)

            # ... length of the response...
            response_cnt = self._num_visis
            self._buffer += struct.pack(">i", response_cnt)

            # ... and the payload
            for i in range(response_cnt):
                self._buffer += struct.pack(">b", fake_visibility_value(i))

    async def readexactly(self, count):
        assert not self._closing

        if FakeSocket.force_recv_result is not None:
            data = FakeSocket.force_recv_result
            if len(data) < count:
                raise asyncio.IncompleteReadError(partial=data, expected=count)
            return data[:count]

        assert len(self._buffer) >= count

        data = self._buffer[0:count]
        self._buffer = self._buffer[count:]
        return data


class FakeStreamWriter:
    def __init__(self, conn):
        self._conn = conn

    def write(self, data):
        self._conn.write(data)

    async def drain(self):
        pass

    def close(self):
        self._conn.close()

    async def wait_closed(self):
        pass

    def is_closing(self):
        return self._conn.is_closing()


class FakeStreamReader:
    def __init__(self, conn):
        self._conn = conn

    async def readexactly(self, count):
        return await self._conn.readexactly(count)


async def fake_open_connection(host, port):
    if FakeSocket.open_connection_exception is not None:
        raise FakeSocket.open_connection_exception
    conn = FakeSocket()
    return FakeStreamReader(conn), FakeStreamWriter(conn)
