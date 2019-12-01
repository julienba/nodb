(ns nodb.io.protocol)

(defprotocol IO
  (store [this path type-maps entries opts])
  (get-meta [this path])
  (get-data [this path])
  (get-config [this path])
  (get-all [this path]))
