from pymodbus.client import AsyncModbusTcpClient


class FakeResponse:
    def __init__(self, registers=None, error=False):
        self._registers = registers if registers is not None else []
        self._error = error

    def isError(self):
        return self._error

    @property
    def registers(self):
        return self._registers


class FakeModbusClient(AsyncModbusTcpClient):
    can_connect = True

    def __init__(self, host, port=0, timeout=0, *args, **kwargs):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._connected = False

    async def connect(self):
        self._connected = self.can_connect
        return self._connected

    def close(self):
        self._connected = False

    @property
    def connected(self):
        return self._connected

    async def _read(self, addr, count):
        if addr == 1000:
            # Error response
            return FakeResponse(error=True)
        elif addr == 1001:
            # Return too little data
            return FakeResponse(registers=[])
        elif addr == 1002:
            # Return too much data
            return FakeResponse(registers=[0] * 16)
        elif addr == 1003:
            # Exception
            raise Exception("Simulated Modbus exception")
        else:
            # Return the addr as value(s)
            values = [addr + i for i in range(count)]
            return FakeResponse(registers=values)

    async def read_holding_registers(self, addr, *, count=1, **kwargs):
        return await self._read(addr, count)

    async def read_input_registers(self, addr, *, count=1, **kwargs):
        return await self._read(addr, count)

    async def write_registers(self, addr, values, **kwargs):
        if addr == 1000:
            # Error response
            return FakeResponse(error=True)
        elif addr == 1001:
            # Exception
            raise Exception("Simulated Modbus exception")
        else:
            return FakeResponse()
