# Higgs Binary Message

The name shamelessly stolen from the almost mythical Higgs Boson http://en.wikipedia.org/wiki/Higgs_boson .

* __Higgs__ - the name of the library
* __Boson__ - the name of a custom binary protocol

Enables de/serialisation of objects to a series of bytes.
Also supports Jackson's JSON tree enabling serialisation of Jackson objects.

Importantly, it supports recursive objects by having support for references. This means there are JSON object trees Jackson will fail to serialise failing with StackoverflowError by Boson will serialise it just fine.
 See [boson](boson/README.md)