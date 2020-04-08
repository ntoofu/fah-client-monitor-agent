# fah-client-monitor-agent

**fah-client-monitor-agent** is a monitor agent to collect metrics from [Folding@home](https://foldingathome.org/) client.

## Build

```
go build
```

## Usage

```
./fah-client-monitor-agent -elasticsearch='http://localhost:9200/' -client-name='hogehoge' -client-port='localhost:36330'
```
