import asyncio
import logging
from pymodbus.client import AsyncModbusTcpClient

from luxtronik.shi.constants import (
    LUXTRONIK_DEFAULT_MODBUS_PORT,
    LUXTRONIK_DEFAULT_MODBUS_TIMEOUT,
    LUXTRONIK_WAIT_TIME_AFTER_HOLDING_WRITE,
)
from luxtronik.shi.common import (
    LuxtronikSmartHomeTelegrams,
    LuxtronikSmartHomeReadHoldingsTelegram,
    LuxtronikSmartHomeReadInputsTelegram,
    LuxtronikSmartHomeWriteHoldingsTelegram,
)


LOGGER = logging.getLogger(__name__)

###############################################################################
# Modbus TCP interface
###############################################################################


class LuxtronikModbusTcpInterface:
    """
    Luxtronik read/write interface using Modbus-TCP.
    This class is designed to offer a simple addr/count/data interface.
    There are functions to read or write individual register blocks,
    or multiple blocks in a row using a list of telegrams.
    The connection is established once and kept open across calls,
    reconnecting only if it was previously closed or dropped.
    """

    def __init__(self, host, port=LUXTRONIK_DEFAULT_MODBUS_PORT, timeout=LUXTRONIK_DEFAULT_MODBUS_TIMEOUT):
        """
        Initialize the Modbus TCP interface for a Luxtronik host.

        Args:
            host (str): Hostname or IP address of the heat pump.
            port (int): TCP port for the Modbus connection
                  (default: LUXTRONIK_DEFAULT_MODBUS_PORT).
            timeout (float): Timeout in seconds for communication
                     (default: LUXTRONIK_DEFAULT_MODBUS_TIMEOUT).
        """
        self._lock = asyncio.Lock()

        self._host = host
        self._port = port
        self._timeout = timeout
        # AsyncModbusTcpClient() requires a running event loop at construction
        # time, so it can't be built here - it is created lazily on first
        # connect(), by which point we're always running inside a coroutine.
        self._client = None

    @property
    def lock(self):
        return self._lock

    async def connect(self):
        """Establish the persistent Modbus connection, if not already connected."""
        async with self._lock:
            await self._connect()

    async def close(self):
        """Explicitly close the Modbus connection."""
        async with self._lock:
            await self._disconnect()

    # Connection methods ##########################################################

    async def _connect(self):
        """
        Establish a connection to the heat pump.

        Returns:
            bool: True if the connection was successfully established,
                False otherwise.
        """
        if self._client is None:
            self._client = AsyncModbusTcpClient(self._host, port=self._port, timeout=self._timeout)

        # Do nothing if client is already connected
        if self._client.connected:
            return True

        connected = await self._client.connect()

        if not connected:
            LOGGER.error(f"Modbus connection failed, could not connect to {self._host}:{self._port}")
            return False

        LOGGER.info(f"Connected to SHI of Luxtronik heat pump {self._host}:{self._port}")
        return True

    async def _disconnect(self):
        """
        Close the connection to the heat pump.

        Returns:
            bool: True (closing the underlying transport does not report failure).
        """
        # Do nothing if never connected or already closed
        if self._client is None or not self._client.connected:
            return True

        self._client.close()
        return True

    # Common read/write methods ###################################################

    async def _read_register(self, read_reg_cb, telegram):
        """
        Read Modbus registers for a single telegrams.

        This method reads the specified number of 16-bit registers (`count`)
        starting at the given Modbus address (`addr`). The address
        is used directly without applying additional offsets.

        The callback function `read_reg_cb` determines whether input or holding
        registers are read. The retrieved data is stored in the telegram's
        `data` field. If an error occurs, the `data` field is None.

        If a non-existent register is read, the entire single read operation fails.

        Args:
            read_reg_cb (Callable):
                Callback used to perform the actual register read.
            telegrams (LuxtronikSmartHomeReadTelegram):
                A single `LuxtronikSmartHomeReadTelegram`.

        Returns:
            bool: True if the read succeeded, False otherwise.
        """
        # Read len(telegram.data) × 16-bit registers from Modbus address telegram.addr
        # A erroneous read usually always leads to data == None
        try:
            response = await read_reg_cb(telegram.addr, count=telegram.count)
            valid = response is not None and not response.isError() and len(response.registers) == telegram.count
            data = response.registers if valid else None
        except Exception as e:
            LOGGER.error(f"Modbus exception: {e}")
            valid = False
            data = None
        telegram.data = data
        if not valid:
            LOGGER.error(f"Modbus read failed: addr={telegram.addr}, count={telegram.count}")
        return valid

    async def _write_register(self, write_reg_cb, telegram):
        """
        Write Modbus registers for a single telegrams.

        The values in `data` are written to 16-bit registers starting
        at the specified Modbus address (`addr`). The address
        is used directly without applying additional offsets.

        The callback function `write_reg_cb` performs the actual write operation
        (currently only valid for holding registers).

        If a non-existent register is written, all data up to this register is written.

        Args:
            write_reg_cb: Callback used to perform the register write.
            telegrams (LuxtronikSmartHomeWriteTelegram):
                A single `LuxtronikSmartHomeWriteTelegram`.

        Returns:
            bool: True if the write succeeded, False otherwise.
        """
        # Write len(telegram.data) × 16-bit registers at Modbus address telegram.addr
        try:
            response = await write_reg_cb(telegram.addr, telegram.data)
            valid = response is not None and not response.isError()
        except Exception as e:
            LOGGER.error(f"Modbus exception: {e}")
            valid = False
        if not valid:
            LOGGER.error(f"Modbus write error: addr={telegram.addr}, data={telegram.data}")
        return valid

    # Holding methods #############################################################

    async def read_holdings(self, addr, count):
        """
        Read `count` holding 16-bit registers starting at the given Modbus
        address `addr`. The address is used directly without additional offsets.

        If a non-existent register is read, the entire single read operation fails.

        Args:
            addr (int): The starting Modbus register address to read from.
            count (int): The number of 16-bit registers to read.

        Returns:
            list[int] | None: On success, returns the read data as a list of integers.
                              On failure, returns None.
        """
        telegram = LuxtronikSmartHomeReadHoldingsTelegram(addr, count)
        success = await self.send(telegram)
        return telegram.data if success else None

    async def write_holdings(self, addr, data):
        """
        Write all values in `data` to 16-bit holding registers starting at the
        given Modbus address `addr`. The address is used directly without
        additional offsets.

        If a non-existent register is written, all data up to this register is written.

        Args:
            addr (int): The starting Modbus register address to write to.
            count (int): The number of 16-bit registers to write.

        Returns:
            bool: True if the write succeeded, False otherwise.
        """
        telegram = LuxtronikSmartHomeWriteHoldingsTelegram(addr, data)
        return await self.send(telegram)

    # Inputs methods ##############################################################

    async def read_inputs(self, addr, count):
        """
        Read `count` input 16-bit registers starting at the given Modbus
        address `addr`. The address is used directly without additional offsets.

        If a non-existent register is read, the entire single read operation fails.

        Args:
            addr (int): The starting Modbus register address to read from.
            count (int): The number of 16-bit registers to read.

        Returns:
            list[int] | None: On success, returns the read data as a list of integers.
                              On failure, returns None.
        """
        telegram = LuxtronikSmartHomeReadInputsTelegram(addr, count)
        success = await self.send(telegram)
        return telegram.data if success else None

    # List methods ################################################################

    async def send(self, telegrams):
        """
        Read/write holdings/inputs registers for one or more telegrams.

        For each read telegram, the specified number of 16-bit registers (`count`)
        is read starting at the given Modbus address (`addr`).
        The retrieved data is stored in the telegram's `data` field.
        On error, the `data` field is None.
        If a non-existent register is read, the entire single read operation fails.

        For each write telegram, the values in `data` are written to 16-bit registers
        starting at the given Modbus address (`addr`).
        If a non-existent register is written, all data up to this register is written.

        The addresses are used directly without applying additional offsets.

        Args:
            telegrams (list[LuxtronikSmartHomeTelegrams] | LuxtronikSmartHomeTelegram):
                A LuxtronikSmartHomeTelegram or a list of them.

        Returns:
            bool: True if all reads/writes succeeded, False otherwise.
        """
        # Normalize to a list of telegrams or validate input
        _telegrams = telegrams
        if isinstance(_telegrams, tuple(LuxtronikSmartHomeTelegrams)):
            _telegrams = [_telegrams]
        elif not isinstance(_telegrams, list) or not all(
            isinstance(t, tuple(LuxtronikSmartHomeTelegrams)) for t in _telegrams
        ):
            LOGGER.warning(
                f"Invalid argument '{telegrams}': expected a " + "LuxtronikSmartHomeTelegram or a list of them."
            )
            return False

        # Prepare data arrays and count total registers
        total_count = 0
        for t in _telegrams:
            t.prepare()
            if t.count > 0:
                total_count += t.count
            else:
                LOGGER.debug(f"No data requested/provided: addr={t.addr}, count={t.count}")

        # Exit the function if no operation is necessary
        if total_count <= 0:
            return True

        # Acquire lock, connect (if needed) and read/write data.
        # The connection is intentionally left open afterwards for reuse by
        # subsequent calls; use close() to tear it down explicitly.
        success = False
        async with self._lock:
            if await self._connect():
                success = True
                was_write = False
                for t in _telegrams:
                    if t.count <= 0:
                        continue

                    # Process telegram type
                    if isinstance(t, LuxtronikSmartHomeReadHoldingsTelegram):
                        reg_cb = self._client.read_holding_registers
                        is_write = False
                    elif isinstance(t, LuxtronikSmartHomeReadInputsTelegram):
                        reg_cb = self._client.read_input_registers
                        is_write = False
                    elif isinstance(t, LuxtronikSmartHomeWriteHoldingsTelegram):
                        reg_cb = self._client.write_registers
                        is_write = True
                    else:
                        # this should never happen
                        assert False, "Telegram type not supported"

                    # Wait a short time when switching from write to read
                    if not is_write and was_write:
                        # Allow the heat pump to process the changes
                        await asyncio.sleep(LUXTRONIK_WAIT_TIME_AFTER_HOLDING_WRITE)

                    # Perform read or write operation
                    if is_write:
                        valid = await self._write_register(reg_cb, t)
                    else:
                        valid = await self._read_register(reg_cb, t)

                    success &= valid
                    was_write = is_write

                # Wait a short time after a write
                if was_write:
                    # Allow the heat pump to process the changes
                    await asyncio.sleep(LUXTRONIK_WAIT_TIME_AFTER_HOLDING_WRITE)

        return success
