Go-LogSink
---

This service collects logs from various services and simply writes them to stdout.

The Kafka-Consumer listens on topic `log.sink` by default, which can be configured in [.env][0] file.

  [0]: https://github.com/TerrexTech/go-logsink/blob/master/.env
