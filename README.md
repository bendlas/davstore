# davstore

A Clojure WebDAV server backed by a git-like blob store and a datomic
database to hold the directory structure.

This started as a demo app for namespaced xml in clojure, but grew pretty practical.

Even though the project is full of core.typed annotations, it can't yet claim to be type checked, because I couldn't make it work yet.

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

## How?

File blobs are stored in files named by the SHA-1 of their content, in a directory radix-tree of depth 1, similar to `.git/objects`. The difference is, that no file header is added before hashing and they are stored uncompressed.

Directory structures are stored in datomic.

## Why ...

### ... steal git's file structure?
The single-level git sha-1 radix tree is Mr. Torvalds' output of what must have been a comparatively long hammocking session over a principally simple question, that he is uniquely qualified to answer: What's the best tradeoff between collision-freeness, name-length, file-seek performance and input performance, with priority to file-seek performance.

### ... not actually be git-compatible?
This project's priority is to maximise serving performance from the blob storage. That rules out the transparent compression.

The file header is also omitted, in order to be able to respond with a plain `java.util.File` object (in contrast to a an nio channel). This should maximise the chance, that zero-copy serving (sendfile(2)) will be empolyed with any given java server.

### ... use datomic?
Because didn't you always want an undo slider on your web resources / network share? It just fits perfectly with content addressed file blobs.

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

#### What's missing?
You tell me :-)

A few thoughts:
- Serialize and restore directory structure from / to blob storage.
- Make core.typed check it.
- Split it up more.

## License

Copyright © 2014 Herwig Hochleitner

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
