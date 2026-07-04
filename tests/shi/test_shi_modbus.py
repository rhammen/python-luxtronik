import pytest
from unittest.mock import patch

from luxtronik.shi.common import (
    LuxtronikSmartHomeReadTelegram,
    LuxtronikSmartHomeReadHoldingsTelegram,
    LuxtronikSmartHomeReadInputsTelegram,
    LuxtronikSmartHomeWriteTelegram,
    LuxtronikSmartHomeWriteHoldingsTelegram,
    LuxtronikSmartHomeTelegrams,
)
from luxtronik.shi.modbus import LuxtronikModbusTcpInterface
from tests.fake import FakeModbusClient


class DummyTelegram(LuxtronikSmartHomeReadTelegram):
    pass


@patch("luxtronik.shi.modbus.LUXTRONIK_WAIT_TIME_AFTER_HOLDING_WRITE", 0)
@patch("luxtronik.shi.modbus.LuxtronikSmartHomeTelegrams", LuxtronikSmartHomeTelegrams | {DummyTelegram})
class TestModbusInterface:
    host = "local_host"
    port = 9876

    @classmethod
    def setup_class(cls):
        cls.modbus_interface = LuxtronikModbusTcpInterface(cls.host, cls.port)
        cls.modbus_interface._client = FakeModbusClient(cls.host, cls.port)
        assert isinstance(cls.modbus_interface._client, FakeModbusClient)

    async def test_connect(self):
        FakeModbusClient.can_connect = True

        # normal connect()
        result = await self.modbus_interface._connect()
        assert result
        assert self.modbus_interface._client.connected

        # repeated connect()
        result = await self.modbus_interface._connect()
        assert result
        assert self.modbus_interface._client.connected

        # normal disconnect()
        result = await self.modbus_interface._disconnect()
        assert result
        assert not self.modbus_interface._client.connected

        # repeated disconnect()
        result = await self.modbus_interface._disconnect()
        assert result
        assert not self.modbus_interface._client.connected

        FakeModbusClient.can_connect = False

        # faulty connect()
        result = await self.modbus_interface._connect()
        assert not result
        assert not self.modbus_interface._client.connected

        # disconnect() after faulty connect()
        result = await self.modbus_interface._disconnect()
        assert result
        assert not self.modbus_interface._client.connected

        FakeModbusClient.can_connect = True

    async def test_no_connection(self):
        FakeModbusClient.can_connect = False

        # Cannot connect to read holdings
        data = LuxtronikSmartHomeReadHoldingsTelegram(0, 1)
        result = await self.modbus_interface.send(data)
        assert not result

        # Cannot connect to write holdings
        data = LuxtronikSmartHomeWriteHoldingsTelegram(0, [1])
        result = await self.modbus_interface.send(data)
        assert not result

        # Cannot connect to read inputs
        data = LuxtronikSmartHomeReadInputsTelegram(0, 1)
        result = await self.modbus_interface.send(data)
        assert not result

        FakeModbusClient.can_connect = True

    async def test_lock(self):
        # asyncio.Lock is not reentrant, unlike the threading.RLock used previously -
        # verify basic acquire/release behavior via the async context manager instead.
        assert not self.modbus_interface.lock.locked()
        async with self.modbus_interface.lock:
            assert self.modbus_interface.lock.locked()
        assert not self.modbus_interface.lock.locked()

    async def test_data_type(self):

        # str
        result = await self.modbus_interface.send("data")
        assert not result

        # int
        result = await self.modbus_interface.send(0)
        assert not result

        # list
        result = await self.modbus_interface.send([2, 1])
        assert not result

        # Read-telegram base class
        t = LuxtronikSmartHomeReadTelegram(1, 1)
        result = await self.modbus_interface.send(t)
        assert not result

        # Read-holdings-telegram class
        t = LuxtronikSmartHomeReadHoldingsTelegram(1, 1)
        result = await self.modbus_interface.send(t)
        assert result

        # Write-telegram base class
        t = LuxtronikSmartHomeWriteTelegram(1, [1])
        result = await self.modbus_interface.send(t)
        assert not result

        # Write-holdings-telegram class
        t = LuxtronikSmartHomeWriteHoldingsTelegram(1, [1])
        result = await self.modbus_interface.send(t)
        assert result

    async def test_no_holdings_read_data(self):
        data_list = [LuxtronikSmartHomeReadHoldingsTelegram(0, 0), LuxtronikSmartHomeReadHoldingsTelegram(0, 0)]

        # Read zero holdings
        result = await self.modbus_interface.send(data_list)
        assert result  # no error when there is no data
        assert data_list[0].data == []
        assert data_list[1].data == []

    async def test_no_holdings_write_data(self):
        data_list = [LuxtronikSmartHomeWriteHoldingsTelegram(0, []), LuxtronikSmartHomeWriteHoldingsTelegram(0, [])]

        # Write zero holdings
        result = await self.modbus_interface.send(data_list)
        assert result  # no error when there is no data

    async def test_no_inputs_read_data(self):
        data_list = [LuxtronikSmartHomeReadInputsTelegram(0, 0), LuxtronikSmartHomeReadInputsTelegram(0, 0)]

        # Read zero inputs
        result = await self.modbus_interface.send(data_list)
        assert result  # no error when there is no data
        assert data_list[0].data == []
        assert data_list[1].data == []

    @pytest.mark.parametrize(
        "addr, count, valid, data",
        [
            (1, 2, True, [1, 2]),
            (5, 3, True, [5, 6, 7]),
            (0, 0, True, []),  # no error when there is no data
            (1000, 2, False, None),  # client has read error
            (1001, 3, False, None),  # client returns to less data
            (1002, 4, False, None),  # client returns to much data
            (1003, 1, False, None),  # exception
        ],
    )
    async def test_read_holdings(self, addr, count, valid, data):
        data_item = LuxtronikSmartHomeReadHoldingsTelegram(addr, count)

        # read holdings via send()
        result = await self.modbus_interface.send(data_item)
        assert result == valid
        assert data_item.data == data

        # read holdings via read_holdings()
        data_arr = await self.modbus_interface.read_holdings(addr, count)
        if valid:
            assert data_arr == data
        else:
            assert data_arr is None

    @pytest.mark.parametrize(
        "addr, count, valid, data",
        [
            (1, 2, True, [1, 2]),
            (5, 3, True, [5, 6, 7]),
            (0, 0, True, []),  # no error when there is no data
            (1000, 2, False, None),  # client has read error
            (1001, 3, False, None),  # client returns to less data
            (1002, 4, False, None),  # client returns to much data
            (1003, 1, False, None),  # exception
        ],
    )
    async def test_read_inputs(self, addr, count, valid, data):
        data_item = LuxtronikSmartHomeReadInputsTelegram(addr, count)

        # read inputs via send()
        result = await self.modbus_interface.send(data_item)
        assert result == valid
        assert data_item.data == data

        # read holdings via read_inputs()
        data_arr = await self.modbus_interface.read_inputs(addr, count)
        if valid:
            assert data_arr == data
        else:
            assert data_arr is None

    @pytest.mark.parametrize(
        "addr, data, valid",
        [
            (1, [1, 2], True),
            (5, [5, 6, 7], True),
            (0, [], True),  # no error when there is no data
            (1000, [8, 9], False),  # Write error
            (1001, [11], False),  # Exception
        ],
    )
    async def test_write_holdings(self, addr, data, valid):
        data_item = LuxtronikSmartHomeWriteHoldingsTelegram(addr, data)

        # write holdings via send()
        result = await self.modbus_interface.send(data_item)
        assert result == valid

        # write holdings via write_holdings()
        result = await self.modbus_interface.write_holdings(addr, data)
        assert result == valid

    async def test_list(self):
        list = [
            LuxtronikSmartHomeReadHoldingsTelegram(3, 7),
            LuxtronikSmartHomeWriteHoldingsTelegram(4, [11, 21]),
            LuxtronikSmartHomeReadInputsTelegram(2, 3),
        ]

        # all valid
        result = await self.modbus_interface.send(list)
        assert result
        assert list[0].data == [3, 4, 5, 6, 7, 8, 9]
        assert list[1].data == [11, 21]
        assert list[2].data == [2, 3, 4]

        # item[2] requests no data
        list[0]._addr = 2
        list[0]._count = 2
        list[2]._count = 0
        result = await self.modbus_interface.send(list)
        assert result
        assert list[0].data == [2, 3]
        assert list[1].data == [11, 21]
        assert list[2].data == []

    async def test_not_defined(self):
        telegram = DummyTelegram(0, 1)

        try:
            await self.modbus_interface.send(telegram)
            assert False
        except Exception:
            pass
