import glob
import os
import queue
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
import pyterrier as pt
import json

ACTION_JSON = "action"
DOCS_JSON = "docs"
DOC_ID_JSON = "docno"
DOC_ID_JSON_LENGTH = 4092
FIELDS_JSON = "fields"

CREATE_INDEX_REQUEST = "create_index"
CLEAR_INDEX_REQUEST = "clear_index"
INDEX_DOCS_REQUEST = "index_docs"
FINISH_INDEXING_REQUEST = "finish_indexing"

INDEX_PATH = "./index"


def clear_index():
    global indexer

    files = glob.glob(INDEX_PATH + '/*')
    for f in files:
        os.remove(f)

    indexer = None


def json_docs_iter(q: queue.Queue):
    while True:
        doc = q.get()

        # print("rcv:", item)

        if doc is None:
            break

        yield doc


def indexing_thread_fun(iter_fun, fields):
    indexer.index(iter_fun, fields=fields)


class IndexRequestHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_POST(self):
        global indexer, fields, docs_queue, indexing_thread

        content_length = int(self.headers['Content-Length'])
        data = self.rfile.read(content_length).decode('utf-8')

        json_data = json.loads(data)

        action = json_data[ACTION_JSON]

        if action == CREATE_INDEX_REQUEST:
            if indexer is None:
                fields = json_data[FIELDS_JSON]
                print(f"Creating index with fields: {fields}...")

                indexer = pt.IterDictIndexer(INDEX_PATH,
                                             meta={'docno': DOC_ID_JSON_LENGTH},
                                             stemmer=pt.index.TerrierStemmer.none, # Matches galago's, otherwise 'porter'
                                             stopwords=pt.index.TerrierStopwords.none, #Matches galago's, otherwise 'terrier',
                                             tokeniser=pt.index.TerrierTokeniser.utf,
                                             type=pt.index.IndexingType.SINGLEPASS)  # Build the inverted index only

                # Note: not really multithreaded, but the IterDictIndexer is multithreaded internally
                indexing_thread = threading.Thread(target=indexing_thread_fun, args=(json_docs_iter(docs_queue),
                                                                                     fields))
                indexing_thread.start()

        elif action == CLEAR_INDEX_REQUEST:
            print("Clearing index...")
            clear_index()
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()

            return

        elif action == INDEX_DOCS_REQUEST:
            docs = json_data[DOCS_JSON]

            for doc in docs:
                # Move the fields array one level above
                for field in doc[FIELDS_JSON]:
                    doc[field] = doc[FIELDS_JSON][field]
                del doc[FIELDS_JSON]

                docs_queue.put(doc)

        elif action == FINISH_INDEXING_REQUEST:
            docs_queue.put(None)
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()

            print("Last document received, waiting for the indexer to finish...")
            # Note: This can take quite a bit (~10 min), as Terrier is going to join all temp files into the final
            #       index,but during testing it always finished before triggering any timeout
            indexing_thread.join()

            sys.exit(0)

        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()


def run(server_class=HTTPServer, handler_class=IndexRequestHandler, port=35000):
    server_address = ('', port)
    httpd = server_class(server_address, lambda *args, **kwargs: handler_class(*args, **kwargs))
    print('Pyterrier Indexer listening on port', port)
    httpd.serve_forever()


def test():
    index = pt.IndexFactory.of(INDEX_PATH)
    for kv in index.getLexicon():
        print("%s (%s) -> %s (%s)" % (
            kv.getKey(), type(kv.getKey()), kv.getValue().toString(), type(kv.getValue())))

    pointer = index.getLexicon()["michael"]
    for posting in index.getInvertedIndex().getPostings(pointer):
        print(posting.toString() + " doclen=%d" % posting.getDocumentLength())

    br = pt.BatchRetrieve(index, wmodel="Tf")
    print(type(br.search("Michael")))
    br.search("Michael").to_csv(sys.stdout)


if __name__ == "__main__":
    global indexer, fields, docs_queue, indexing_thread

    pt.init()

    # globals initialization
    indexer = None
    fields = None
    docs_queue: queue.Queue = queue.Queue()
    indexing_thread = None

    run()
