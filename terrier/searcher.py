# Searcher for (py)Terrier, which communicates with Knowgly's executable

import multiprocessing
import sys
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

import pandas as pd
import pyterrier as pt
import json

FIELDS_JSON = "fields"
QUERY_JSON = "query"
QUERIES_JSON = "queries"
WEIGHTS_JSON = "weights"

ACTION = "action"
SEARCH_BM25 = "bm25"
SEARCH_BM25F = "bm25f"
SEARCH_BM25F_BULK = "bm25f_bulk"
SEARCH_BM25_BULK = "bm25_bulk"

CREATE_INDEX_REQUEST = "create_index"

INDEX_PATH = "./index"

# You may need to adjust the number of threads, the searcher itself is
# quite resource-hungry (~1GiB per thread)
N_THREADS_BULK_RETRIEVER = multiprocessing.cpu_count() // 2

class IndexRequestHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def get_retriever(self, json_data, action):
        global index, fields_in_index, bm25_retriever, bm25f_retriever

        retriever = None

        if action == SEARCH_BM25:
            retriever = pt.BatchRetrieve(index, wmodel="BM25")

        elif action == SEARCH_BM25F:
            fields = json_data[FIELDS_JSON]
            weights = json_data[WEIGHTS_JSON]

            controls = dict()
            for i, field in enumerate(fields_in_index):
                # Look it up in fields
                try:
                    fieldIdx = fields.index(field)
                    controls["w." + str(i)] = weights[fieldIdx]
                except:
                    controls["w." + str(i)] = 0.0

                controls["c." + str(i)] = 0.9 # b parameter

            retriever = pt.BatchRetrieve(index,
                                         wmodel='BM25F',
                                         controls=controls)

        elif action == SEARCH_BM25F_BULK:
            fields = json_data[FIELDS_JSON]
            weights = json_data[WEIGHTS_JSON]

            controls = dict()
            for i, field in enumerate(fields_in_index):
                # Look it up in fields
                try:
                    fieldIdx = fields.index(field)
                    controls["w." + str(i)] = weights[fieldIdx]
                except:
                    controls["w." + str(i)] = 0.0

                controls["c." + str(i)] = 0.9 # b parameter

            print("controls:", controls)
            print("index.getCollectionStatistics().getFieldNames():", index.getCollectionStatistics().getFieldNames())

            retriever = pt.BatchRetrieve(index,
                                         wmodel='BM25F',
                                         controls=controls).parallel(N_THREADS_BULK_RETRIEVER)


        elif action == SEARCH_BM25_BULK:
            retriever = pt.BatchRetrieve(index, wmodel="BM25").parallel(N_THREADS_BULK_RETRIEVER)

        return retriever

    def do_POST(self):
        global index, fields_in_index, bm25_retriever, bm25f_retriever

        content_length = int(self.headers['Content-Length'])
        data = self.rfile.read(content_length).decode('utf-8')


        json_data = json.loads(data)

        action = json_data[ACTION]
        retriever = self.get_retriever(json_data, action)

        # Tokenize the query with Terrier's default english tokenizer
        # Fixes https://github.com/terrier-org/pyterrier/issues/376
        pipe = pt.rewrite.tokenise(pt.index.TerrierTokeniser.utf) >> retriever

        if action == SEARCH_BM25F_BULK or action == SEARCH_BM25_BULK:
            queries = json_data[QUERIES_JSON]

            queryDf = pd.DataFrame(queries, columns=["qid", "query"])
            res = pipe.transform(queryDf)

            results_by_query = res.groupby('qid')['docid'].agg(list).reset_index()
            scores_by_query = res.groupby('qid')['score'].agg(list).reset_index()

            results_by_query_dict = dict(zip(results_by_query['qid'], results_by_query['docid']))
            scores_by_query_dict = dict(zip(scores_by_query['qid'], scores_by_query['score']))

            for qid in results_by_query_dict.keys():
                docids = results_by_query_dict[qid]
                uris = index.getMetaIndex().getItems("docno", docids)

                results_by_query_dict[qid] = [[uri, scores_by_query_dict[qid][i]] for i, uri in enumerate(uris)]

            # Add empty entries for queries with no results
            for qid in queryDf["qid"]:
                if qid not in results_by_query_dict:
                    results_by_query_dict[qid] = []

            # print(results_by_query_dict)
            json_response = json.dumps({"results": results_by_query_dict}, ensure_ascii=False).encode('utf8')

            # print(json_response.decode())

            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(json_response)

        else:
            query = json_data[QUERY_JSON]

            search_results = pipe.search(query)

            docids = search_results["docid"].to_list()
            scores = search_results["score"].to_list()

            # Do it this way, as for some reason the UTF-8 URIs in the result dataframes are completely broken
            uris = index.getMetaIndex().getItems("docno", docids)

            # Convert the search_results list forcing it (again...) to be a valid UTF-8 JSON string
            json_response = json.dumps({"results": uris, "scores": scores}, ensure_ascii=False).encode('utf8')

            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(json_response)


def run(server_class=ThreadingHTTPServer, handler_class=IndexRequestHandler, port=35000):
    server_address = ('', port)
    httpd = server_class(server_address, lambda *args, **kwargs: handler_class(*args, **kwargs))
    print('Pyterrier Searcher listening on port', port)
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
    global index, fields_in_index

    pt.init()

    # globals initialization
    index = pt.IndexFactory.of(INDEX_PATH)
    fields_in_index = index.getCollectionStatistics().getFieldNames()

    run()
