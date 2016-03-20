[![build-status](https://travis-ci.org/isnok/data-pypes.svg?branch=master)](https://travis-ci.org/isnok/data-pypes)


# The DataPypes framework

Another attempt to structure processes.

## Documentation

To make this hands-on useful, it is mostly documented in the code.

## Play around

Here are some interesting invocations of `pypes-example.py`.

```shell
./pypes-example.py
...
LOGLEVEL=DEBUG ./pypes-example.py
...
LOGLEVEL=debug STDOUT_LOGLEVEL=error ./pypes-example.py
...
LOGLEVEL=debug STDOUT_LOGLEVEL=info ERROR_LOGFILE=error.log ./pypes-example.py
...
STDOUT_LOGLEVEL=success WARNING_LOGFILE=warn.log ./pypes-example.py
...
```

## Usage

The ideas is very general.
It was intended to structure "long" comutational processes into reusable elements.

The file `logsetup.py` can also be use stand-alone.
