job "tcp-ais-broker" {
  datacenters = ["dc1"]
  type = "service"

  group "broker" {
    count = 1

    network {
      port "downstream" {
        static = 9001
      }
      port "metrics" {
        static = 9101
      }
    }

    service {
      name = "tcp-ais-broker"
      port = "downstream"

      check {
        name     = "ready"
        type     = "http"
        path     = "/ready"
        port     = "metrics"
        interval = "10s"
        timeout  = "2s"
      }
    }

    task "broker" {
      driver = "raw_exec"

      artifact {
        source      = "http://192.168.99.107:9000/binaries/tcp-ais-broker"
        destination = "local/tcp-ais-broker"
        mode        = "file"
      }

      config {
        command = "local/tcp-ais-broker"
        args = [
          "--upstream-host", "153.44.253.27",
          "--upstream-port", "5631",
          "--listen", "0.0.0.0:9001",
          "--metrics-listen", "0.0.0.0:9101",
          "--framing", "line",
          "--ais-multipart-mode", "affinity",
          "--queue-max-messages", "100000",
          "--no-consumer-policy", "buffer",
        ]
      }
    }
  }
}
