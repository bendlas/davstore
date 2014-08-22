(defproject net.bendlas/davstore "0.1.0-SNAPSHOT"
  :description "A file storage component with three parts:
    - A blob store, storing in a git-like content-addressing scheme
    - A datomic schema to store blob references with metadata
    - A connector to expose files over webdav"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0-alpha1"]
                 [webnf/base "0.0.12"]
                 [webnf/handler "0.0.11"]
                 [webnf/datomic "0.1.0-SNAPSHOT"]
                 [webnf/enlive.clj "0.0.3"]
                 [webnf.deps/contrib "0.0.3"]
                 [org.clojure/data.xml "9999-bendlas-SNAPSHOT"]
                 [org.clojure/core.typed "0.2.66"]
                 [de.kotka/lazymap "3.1.1"]
                 [cider/cider-nrepl "0.8.0-SNAPSHOT"]]
  :plugins [[lein-ring "0.8.11"]]
  :ring {:handler davstore.app/davstore
         :nrepl {:start? true :port 4012}})
