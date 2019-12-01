(defproject nodb "0.1.0-SNAPSHOT"
  :description "Making data analyse faster at any scale."
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [com.damballa/abracad "0.4.13"]
                 [org.clojure/data.csv "0.1.4"]
                 [clojure.java-time "0.3.2"]
                 [tesser.math "1.0.3"]
                 [proto-repl-charts "0.3.2"]])
  ;:repl-options {:init-ns nodb.core})
