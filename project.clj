(defproject davstore "0.1.0-SNAPSHOT"
  :description "A file storage component with three parts:
    - A blob store, storing in a git-like content-addressing scheme
    - A datomic schema to store blob references with metadata
    - A connector to expose files over webdav"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories [["bendlas-nexus" {:url "http://nexus.bendlas.net/content/groups/public"
                                   :username "fetch" :password :gpg}]]
  :dependencies [[org.clojure/clojure "1.7.0-alpha4"]
                 [org.clojure/core.typed "0.2.72"]
                 [net.bendlas/data.xml "1.0.0-SNAPSHOT"]
                 [webnf/base "0.1.0-alpha3"
                  :exclusions [org.clojure/data.xml]]
                 [webnf/handler "0.1.0-alpha3"]
                 [webnf/datomic "0.1.0-alpha3"]
                 [webnf/enlive.clj "0.1.0-alpha3"]
                 [webnf.deps/universe "0.1.0-alpha3"
                  :exclusions [org.clojure/data.xml]]
                 [ring/ring-jetty-adapter "1.3.1"]
                 [cider/cider-nrepl "0.8.1"]
                 [com.datomic/datomic-free "0.9.5067"]]
  :plugins [[lein-ring "0.8.11"]]
  :ring {:handler davstore.app/davstore
         :nrepl {:start? true :port 4012}})
