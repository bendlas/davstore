# davstore

A Clojure WebDAV server backed by a git-like blob store and a datomic
database to hold the directory structure.

## Usage

To quickly start a server from a checkout

    git clone https://github.com/bendlas/davstore.git
	cd davstore
    lein ring server-headless 8080

This creates an in-memory datomic database for the file system and
stores the file blobs in `/tmp/davstore-app`.
The default webdav root is `#uuid "7178245f-5e6c-30fb-8175-c265b9fe6cb8"`

For other use cases, please check out the code in `davstore.app` and FAQ.
To use it as a library, add it to your dependencies:

	[davstore "0.1.0-SNAPSHOT"]

and use it as a ring handler:

    (require '[davstore.app :refer [wrap-store file-handler]])
	(def handler (wrap-store file-handler
	               "/var/db/blobs"
	               "datomic:free://localhost:4334/your-store"
				   "/uri/prefix/"))

## FAQ

#### Can I use it without datomic?
Right now, only the blob store and the XML parser for WebDAV are fully decoupled.
So, in order to use the WebDAV ring handler with a different database, you will have to wait for a version with an intermediate data format for webdav requests between the handler and the storage backend.

That said, just having the blob store handy is a big win already and that is a single, leaf namespace with slim dependencies.

Pull Request welcome.

#### Why is the default root `#uuid "7178245f-5e6c-30fb-8175-c265b9fe6cb8"`?
It's the version 5 (named by sha-1) uuid of the clojure symbol `davstore.app/root-id`:
```
(java.util.UUID/nameUUIDFromBytes
  (.digest (java.security.MessageDigest/getInstance "SHA-1")
           (.getBytes (str 'davstore.app/root-id) "UTF-8")))
```
This is also the symbol, that it's bound to in the API.

#### Why don't your core.typed annotations typecheck?
I'm waiting on outstanding issues of core.typed and haven't bothered to remove the
(hopefully still correct) annotations.

## License

Copyright © 2014 Herwig Hochleitner

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
