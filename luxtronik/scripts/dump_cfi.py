#! /usr/bin/env python3

# pylint: disable=invalid-name
"""
Script to dump all available config interface values of the Luxtronik controller
"""

import asyncio

from luxtronik import LuxtronikSocketInterface, LUXTRONIK_DEFAULT_PORT
from luxtronik.scripts import create_default_args_parser, dump_fields


def dump_all(data):
    dump_fields(data.parameters)
    dump_fields(data.calculations)
    dump_fields(data.visibilities)


async def dump_cfi_async():
    parser = create_default_args_parser(
        "Dumps all config interface values of the Luxtronik controller", LUXTRONIK_DEFAULT_PORT
    )
    args = parser.parse_args()
    print(f"Dump CFI of {args.ip}:{args.port}")
    async with LuxtronikSocketInterface(args.ip, args.port) as client:
        data = await client.read()
        dump_all(data)


def dump_cfi():
    asyncio.run(dump_cfi_async())


if __name__ == "__main__":
    dump_cfi()
