(defproject davstore "0.1.0-alpha6"
  :description "A file storage component with three parts:
    - A blob store, storing in a git-like content-addressing scheme
    - A datomic schema to store blob references with metadata
    - A connector to expose files over webdav"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  #_#_:repositories [["bendlas-nexus" {:url "http://nexus.bendlas.net/content/groups/public"
                                   :username "fetch" :password :gpg}]]
  :dependencies [[org.clojure/clojure "1.7.0-RC2"]
                 [net.bendlas/data.xml "1.0.0-20141001.050108-3"]
                 [webnf/base "0.1.15"
                  :exclusions [org.clojure/data.xml]]
                 [webnf/handler "0.1.15"]
                 [webnf/datomic "0.1.15"]
                 [webnf/enlive.clj "0.1.15"]
                 [webnf.deps/universe "0.1.15"
                  :exclusions [org.clojure/data.xml]]
                 [ring/ring-jetty-adapter "1.4.0-RC1"]]
  :plugins [[lein-ring "0.9.6"]]
  :ring {:handler davstore.app/davstore
         :nrepl {:start? true :port 4012}})
