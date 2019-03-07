[![license](http://img.shields.io/badge/license-Apache%20v2-orange.svg)](https://raw.githubusercontent.com/Peltoche/ical-rs/master/LICENSE)
[![GoDoc](https://godoc.org/github.com/leboncoin/schema-registry?status.svg)](https://godoc.org/github.com/leboncoin/schema-registry)
[![Build Status](https://travis-ci.org/leboncoin/schema-registry.svg?branch=master)](https://travis-ci.org/leboncoin/schema-registry)
[![codecov](https://codecov.io/gh/leboncoin/schema-registry/branch/master/graph/badge.svg)](https://codecov.io/gh/leboncoin/schema-registry)
[![Go Report Card](https://goreportcard.com/badge/github.com/leboncoin/schema-registry)](https://goreportcard.com/report/github.com/leboncoin/schema-registry)

# Go client for Schema Registry

A rewrite of [Landdop/schema-registry](https://github.com/Landoop/schema-registry)
for the Schema Registry v5.1.1 and above

## Why ?

The [Landdop/schema-registry](https://github.com/Landoop/schema-registry) project have
been created in 2016 and a lot of things have changed since that time. This rewrite
aims to be a more up-to-date client by adding the following features:

- It keep the method signature similar to the Landoop client for an easy migration
- It does support the contexts for all the requests
- It support the v5.1.1 endpoints like the schema deletion and the compatibility checking
- It keep its code simple
- It leave the gzip managment to the go client
- It keep everything tested
- It propose a mock
