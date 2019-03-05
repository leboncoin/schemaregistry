[![codecov](https://codecov.io/gh/leboncoin/schema-registry/branch/master/graph/badge.svg)](https://codecov.io/gh/leboncoin/schema-registry)

# Go client for Schema Registry

A modern rewrite of [Landdop/schema-registry](https://github.com/Landoop/schema-registry)
for the Schema Registry V5.1.1 and above

## Why ?

The [Landdop/schema-registry](https://github.com/Landoop/schema-registry) project have
been created in 2016 and a lot of things have changed since that time. This rewrite
aims to be a more up-to-date client by adding the following features:

- It does support the contexts for all the requests
- It support the v5.1.1 endpoints like the schema deletion
- It try to keep its code simple
- It leave the gzip managment to the go client
- It keep everything tested
- It propose a mock
