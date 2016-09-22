# Gq

A graph query engine with support for embedded and distributed backends. Gq scales to large datasets by leveraging Google's
Bigtable database.

## Goals

1. Scale out through distributed backends
2. Simple to operate
3. Low latency insertion and query performance


## Overview

Gq is a graph query engine. It store's your graph based data in any key value store that supports fast iteration over
keys. Gq follows the property graph model where relationships may also have properties attached to them as well as nodes.
Gq combines a simple query language expressing traversals in Json that can easily be used to build client libraries.

## Quickstart


