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


## Example


```go
import "github.com/landoop/schema-registry"

client, _ := schemaregistry.NewClient("http://localhost:8081")
client.Subjects()
```

Or, to use with a Schema Registry endpoint listening on HTTPS:

```go
import (
    "crypto/tls"
    "crypto/x509"
    "io/ioutil"

    "github.com/landoop/schema-registry"
)

// Create a TLS config to use to connect to Schema Registry. This config will permit TLS connections to an endpoint
// whose TLS cert is signed by the given caFile.
caCert, err := ioutil.ReadFile("/path/to/ca/file")
if err != nil {
    panic(err)
}

caCertPool := x509.NewCertPool()
caCertPool.AppendCertsFromPEM(caCert)

tlsConfig :=  &tls.Config{
    RootCAs:            caCertPool,
    InsecureSkipVerify: true,
}

httpsClientTransport := &http.Transport{
  TLSClientConfig: tlsConfig,
}

httpsClient := &http.Client{
  Transport: httpsClientTransport,
}

// Create the Schema Registry client
client, _ := schemaregistry.NewClient("https://localhost:8081", UsingClient(httpsClient))
client.Subjects()
```
