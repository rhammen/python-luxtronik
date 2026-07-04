"""Main components of the Luxtronik config interface."""

import asyncio
import logging
import socket
import struct

from luxtronik.collections import integrate_data
from luxtronik.cfi.constants import (
    LUXTRONIK_DEFAULT_PORT,
    LUXTRONIK_DEFAULT_TIMEOUT,
    LUXTRONIK_MAX_RETRIES,
    LUXTRONIK_RETRY_DELAY,
    LUXTRONIK_PARAMETERS_WRITE,
    LUXTRONIK_PARAMETERS_READ,
    LUXTRONIK_CALCULATIONS_READ,
    LUXTRONIK_VISIBILITIES_READ,
    LUXTRONIK_SOCKET_READ_SIZE_INTEGER,
    LUXTRONIK_SOCKET_READ_SIZE_CHAR,
    WAIT_TIME_AFTER_PARAMETER_WRITE,
    LUXTRONIK_CFI_REGISTER_BIT_SIZE,
)
from luxtronik.cfi.calculations import Calculations
from luxtronik.cfi.parameters import Parameters
from luxtronik.cfi.visibilities import Visibilities


LOGGER = logging.getLogger(__name__)

###############################################################################
# Config interface data
###############################################################################


class LuxtronikData:
    """
    Collection of parameters, calculations and visiblities.
    Also provide some high level access functions to their data values.
    """

    def __init__(self, parameters=None, calculations=None, visibilities=None, safe=True):
        self.parameters = Parameters(safe) if parameters is None else parameters
        self.calculations = Calculations() if calculations is None else calculations
        self.visibilities = Visibilities() if visibilities is None else visibilities

    def get_firmware_version(self):
        return self.calculations.get_firmware_version()


###############################################################################
# Config interface
###############################################################################


class LuxtronikSocketInterface:
    """Luxtronik read/write interface via socket."""

    def __init__(self, host, port=LUXTRONIK_DEFAULT_PORT, timeout=LUXTRONIK_DEFAULT_TIMEOUT):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._connection_lock = asyncio.Lock()
        self._reader = None
        self._writer = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
        return False

    async def connect(self):
        """Establish the persistent connection to the heat pump, if not already connected."""
        async with self._connection_lock:
            await self._ensure_connected()

    async def close(self):
        """Explicitly close the connection to the heat pump."""
        async with self._connection_lock:
            await self._disconnect()

    async def _ensure_connected(self):
        "Low-level helper to (re)open the persistent connection if needed"
        if self._writer is not None and not self._writer.is_closing():
            return
        await self._disconnect()
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(self._host, self._port), timeout=self._timeout
        )
        LOGGER.info("Connected to CFI of Luxtronik heat pump %s:%s", self._host, self._port)

    async def _disconnect(self):
        "Low-level helper to tear down the persistent connection, if any"
        if self._writer is not None:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except OSError:
                pass
        self._reader = None
        self._writer = None

    async def _with_lock_and_connect(self, func, *args, **kwargs):
        """
        Wrapper around various read/write functions to ensure a connection first.

        Locking is being used to ensure that only a single operation is performed
        at any point in time on this instance's persistent connection. Transient
        connection/protocol errors are retried a limited number of times, with a
        short delay and a reconnect in between, before giving up.
        """
        async with self._connection_lock:
            ret_val = None
            for attempt in range(LUXTRONIK_MAX_RETRIES + 1):
                try:
                    await self._ensure_connected()
                    return await func(*args, **kwargs)
                except socket.gaierror as e:
                    LOGGER.error(
                        "Failed to connect to Luxtronik heat pump %s:%s. %s.",
                        self._host,
                        self._port,
                        f"Address-related error: {e}",
                    )
                except asyncio.TimeoutError as e:
                    LOGGER.error(
                        "Failed to communicate with Luxtronik heat pump %s:%s. %s.",
                        self._host,
                        self._port,
                        f"Operation timed out: {e}",
                    )
                except ConnectionRefusedError as e:
                    LOGGER.error(
                        "Failed to connect to Luxtronik heat pump %s:%s. %s.",
                        self._host,
                        self._port,
                        f"Connection refused: {e}",
                    )
                except (OSError, asyncio.IncompleteReadError) as e:
                    LOGGER.error(
                        "Failed to communicate with Luxtronik heat pump %s:%s. %s.",
                        self._host,
                        self._port,
                        f"OS/protocol error: {e}",
                    )
                except Exception as e:
                    LOGGER.error(
                        "Failed to communicate with Luxtronik heat pump %s:%s. %s.",
                        self._host,
                        self._port,
                        f"Unknown exception: {e}",
                    )
                    await self._disconnect()
                    return None
                await self._disconnect()
                if attempt < LUXTRONIK_MAX_RETRIES:
                    LOGGER.warning(
                        "%s: Retrying in %s second(s) (attempt %d/%d)...",
                        self._host,
                        LUXTRONIK_RETRY_DELAY,
                        attempt + 1,
                        LUXTRONIK_MAX_RETRIES + 1,
                    )
                    await asyncio.sleep(LUXTRONIK_RETRY_DELAY)
            return ret_val

    async def read(self, data=None):
        """
        All available data will be read from the heat pump
        and integrated to the passed data object.
        This data object is returned afterwards, mainly for access to a newly created.
        """
        if data is None:
            data = LuxtronikData()
        return await self._with_lock_and_connect(self._read, data)

    async def read_parameters(self, parameters=None):
        """
        Read parameters from heat pump and integrate them to the passed dictionary.
        This dictionary is returned afterwards, mainly for access to a newly created.
        """
        if parameters is None:
            parameters = Parameters()
        return await self._with_lock_and_connect(self._read_parameters, parameters)

    async def read_calculations(self, calculations=None):
        """
        Read calculations from heat pump and integrate them to the passed dictionary.
        This dictionary is returned afterwards, mainly for access to a newly created.
        """
        if calculations is None:
            calculations = Calculations()
        return await self._with_lock_and_connect(self._read_calculations, calculations)

    async def read_visibilities(self, visibilities=None):
        """
        Read visibilities from heat pump and integrate them to the passed dictionary.
        This dictionary is returned afterwards, mainly for access to a newly created.
        """
        if visibilities is None:
            visibilities = Visibilities()
        return await self._with_lock_and_connect(self._read_visibilities, visibilities)

    async def write(self, parameters):
        """
        Write all set parameters to the heat pump.
        :param Parameters() parameters  Parameter dictionary to be written
                          to the heatpump before reading all available data
                          from the heat pump.
        """
        await self._with_lock_and_connect(self._write, parameters)

    async def write_and_read(self, parameters, data=None):
        """
        Write all set parameter to the heat pump (see write())
        prior to reading back in all data from the heat pump (see read())
        after a short wait time
        """
        if data is None:
            data = LuxtronikData()
        return await self._with_lock_and_connect(self._write_and_read, parameters, data)

    async def _read(self, data):
        await self._read_parameters(data.parameters)
        await self._read_calculations(data.calculations)
        await self._read_visibilities(data.visibilities)
        return data

    async def _write_and_read(self, parameters, data):
        await self._write(parameters)
        return await self._read(data)

    async def _write(self, parameters):
        if not isinstance(parameters, Parameters):
            LOGGER.error("Only parameters are writable!")
            return
        count = 0
        for definition, field in parameters.items():
            if field.write_pending:
                field.write_pending = False
                value = field.raw
                if not isinstance(definition.index, int) or not field.check_for_write(parameters.safe):
                    LOGGER.warning(
                        "%s: Parameter id '%s' or value '%s' invalid!",
                        self._host,
                        definition.index,
                        value,
                    )
                    continue
                LOGGER.debug("%s: Parameter '%d' set to '%s'", self._host, definition.index, value)
                await self._send_ints(LUXTRONIK_PARAMETERS_WRITE, definition.index, value)
                cmd = await self._read_int()
                LOGGER.debug("%s: Command %s", self._host, cmd)
                val = await self._read_int()
                LOGGER.debug("%s: Value %s", self._host, val)
                count += 1
        LOGGER.info("%s: Write %d parameters", self._host, count)
        # Give the heatpump a short time to handle the value changes/calculations:
        await asyncio.sleep(WAIT_TIME_AFTER_PARAMETER_WRITE)

    async def _read_parameters(self, parameters):
        data = []
        await self._send_ints(LUXTRONIK_PARAMETERS_READ, 0)
        cmd = await self._read_int()
        LOGGER.debug("%s: Command %s", self._host, cmd)
        length = await self._read_int()
        LOGGER.debug("%s: Length %s", self._host, length)
        for _ in range(0, length):
            data.append(await self._read_int())
        LOGGER.info("%s: Read %d parameters", self._host, length)
        self._parse(parameters, data)
        return parameters

    async def _read_calculations(self, calculations):
        data = []
        await self._send_ints(LUXTRONIK_CALCULATIONS_READ, 0)
        cmd = await self._read_int()
        LOGGER.debug("%s: Command %s", self._host, cmd)
        stat = await self._read_int()
        LOGGER.debug("%s: Stat %s", self._host, stat)
        length = await self._read_int()
        LOGGER.debug("%s: Length %s", self._host, length)
        for _ in range(0, length):
            data.append(await self._read_int())
        LOGGER.info("%s: Read %d calculations", self._host, length)
        self._parse(calculations, data)
        return calculations

    async def _read_visibilities(self, visibilities):
        data = []
        await self._send_ints(LUXTRONIK_VISIBILITIES_READ, 0)
        cmd = await self._read_int()
        LOGGER.debug("%s: Command %s", self._host, cmd)
        length = await self._read_int()
        LOGGER.debug("%s: Length %s", self._host, length)
        for _ in range(0, length):
            data.append(await self._read_char())
        LOGGER.info("%s: Read %d visibilities", self._host, length)
        self._parse(visibilities, data)
        return visibilities

    async def _send_ints(self, *ints):
        "Low-level helper to send a tuple of ints"
        data = struct.pack(">" + "i" * len(ints), *ints)
        LOGGER.debug("%s: sending %s", self._host, data)
        self._writer.write(data)
        await asyncio.wait_for(self._writer.drain(), timeout=self._timeout)

    async def _read_bytes(self, count):
        "Low-level helper to receive a precise number of bytes"
        try:
            return await asyncio.wait_for(self._reader.readexactly(count), timeout=self._timeout)
        except asyncio.IncompleteReadError as e:
            LOGGER.error("%s: Connection died.", self._host)
            raise ConnectionError("Connection to %s died." % self._host) from e

    async def _read_int(self):
        "Low-level helper to receive an int"
        reading = await self._read_bytes(LUXTRONIK_SOCKET_READ_SIZE_INTEGER)
        return struct.unpack(">i", reading)[0]

    async def _read_char(self):
        "Low-level helper to receive a signed int"
        reading = await self._read_bytes(LUXTRONIK_SOCKET_READ_SIZE_CHAR)
        return struct.unpack(">b", reading)[0]

    def _parse(self, data_vector, raw_data):
        """
        Parse raw data into the corresponding fields.

        Args:
            data_vector (DataVector): Data vector in which
                the raw data is to be integrated.
            raw_data (list[int]): List of raw register values.
                The raw data must start at register index 0.
        """
        raw_len = len(raw_data)
        # Prepare a list of undefined indices
        undefined = {i for i in range(0, raw_len)}

        # integrate the data into the fields
        for pair in data_vector.data.items():
            definition, field = pair
            # skip this field if there are not enough data
            next_idx = definition.index + definition.count
            if next_idx > raw_len:
                # not enough registers
                field.clear()
                continue
            # remove all used indices from the list of undefined indices
            for index in range(definition.index, next_idx):
                undefined.discard(index)
            # integrate_data() also resets the write_pending flag,
            # intentionally only for read fields
            pair.integrate_data(raw_data, LUXTRONIK_CFI_REGISTER_BIT_SIZE)

        # create an unknown field for additional data
        for index in undefined:
            # LOGGER.warning(f"Entry '%d' not in list of {self.name}", index)
            definition = data_vector.definitions.create_unknown_definition(index)
            field = definition.create_field()
            integrate_data(definition, field, raw_data, LUXTRONIK_CFI_REGISTER_BIT_SIZE, index)
            data_vector.data.add_sorted(definition, field)
