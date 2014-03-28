"""Pydatacube - a library for handling statistical data tables

Pydatacube offers a simple API for handling statistical data
tables, especially those that are in a so called "cube format". Although
efficient in terms of computation and space, the cube format can be a bit
tedious to work with as is. Pydatacube simplifies working with such data
by exposing them over an API that feels like working with a two-dimensional
table (think CSV).

Most of the stuff is done in the _DataCube class, but to get data
in (and out), see converter modules pydatacube.jsonstat and
pydatacube.pcaxis.
"""
