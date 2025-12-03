"""
Driver for all PySpark apps

Jobs should be packaged using a wheel file, which includes the entry point for the Spark App.
This driver serves as a simple wrapper to invoke any function in the wheel.
"""
import sys
import importlib

if __name__ == "__main__":
    argv = sys.argv
    if len(argv) < 3:
        raise RuntimeError("Usage: driver.py <module> <func_name> [-- <func_args...>]")
    module = importlib.import_module(argv[1])
    entrypoint = getattr(module, argv[2])
    if len(argv) > 3:
        assert argv[3] == "--", "Please use '--' as a separator for passing args for your entry point function"
        sys.argv = argv[3:]
    entrypoint()
