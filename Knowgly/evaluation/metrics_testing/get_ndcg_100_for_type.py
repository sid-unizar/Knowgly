import sys
import subprocess
import matplotlib.pyplot as plt

QUERY_RESULTS_FILE = sys.argv[1]
TYPE = sys.argv[2]

QRELS_FILE = None
if TYPE == "Person":
    QRELS_FILE = "../person_purged.txt"
elif TYPE == "Organisation":
    QRELS_FILE = "../organisation_purged.txt"
elif TYPE == "Place":
    QRELS_FILE = "../place_purged.txt"

trec_output = subprocess.check_output(["./trec_eval", "-q", "-m", "ndcg_cut.100", QRELS_FILE, QUERY_RESULTS_FILE])

# Note: This has been manually annotated
dbpedia_entity_types = {
    "INEX_LD-20120111" : "Other",
    "INEX_LD-20120112" : "Other",
    "INEX_LD-20120121" : "Other",
    "INEX_LD-20120122" : "Other",
    "INEX_LD-20120131" : "Place",
    "INEX_LD-20120132" : "Place",
    "INEX_LD-20120211" : "Other",
    "INEX_LD-20120212" : "Other",
    "INEX_LD-20120221" : "Other",
    "INEX_LD-20120222" : "Other",
    "INEX_LD-20120231" : "Other",
    "INEX_LD-20120232" : "Other",
    "INEX_LD-20120311" : "Other",
    "INEX_LD-20120312" : "Place",
    "INEX_LD-20120321" : "Person",
    "INEX_LD-20120322" : "Other",
    "INEX_LD-20120331" : "Other",
    "INEX_LD-20120332" : "Other",
    "INEX_LD-20120411" : "Other",
    "INEX_LD-20120412" : "Other",
    "INEX_LD-20120421" : "Place",
    "INEX_LD-20120422" : "Other",
    "INEX_LD-20120431" : "Other",
    "INEX_LD-20120432" : "Other",
    "INEX_LD-20120511" : "Person",
    "INEX_LD-20120512" : "Organisation",
    "INEX_LD-20120521" : "Other",
    "INEX_LD-20120522" : "Other",
    "INEX_LD-20120531" : "Other",
    "INEX_LD-20120532" : "Organisation",
    "INEX_LD-2009022" : "Other",
    "INEX_LD-2009039" : "Other",
    "INEX_LD-2009053" : "Organisation",
    "INEX_LD-2009061" : "Other",
    "INEX_LD-2009062" : "Other",
    "INEX_LD-2009063" : "Other",
    "INEX_LD-2009074" : "Other",
    "INEX_LD-2009096" : "Other",
    "INEX_LD-2009111" : "Place",
    "INEX_LD-2009115" : "Place",
    "INEX_LD-2010004" : "Other",
    "INEX_LD-2010014" : "Place",
    "INEX_LD-2010019" : "Other",
    "INEX_LD-2010020" : "Other",
    "INEX_LD-2010037" : "Other",
    "INEX_LD-2010043" : "Other",
    "INEX_LD-2010057" : "Other",
    "INEX_LD-2010069" : "Other",
    "INEX_LD-2010100" : "Other",
    "INEX_LD-2010106" : "Other",
    "INEX_LD-2012301" : "Place",
    "INEX_LD-2012303" : "Other",
    "INEX_LD-2012305" : "Place",
    "INEX_LD-2012307" : "Person",
    "INEX_LD-2012309" : "Person",
    "INEX_LD-2012311" : "Other",
    "INEX_LD-2012313" : "Other",
    "INEX_LD-2012315" : "Other",
    "INEX_LD-2012317" : "Other",
    "INEX_LD-2012318" : "Person",
    "INEX_LD-2012319" : "Other",
    "INEX_LD-2012321" : "Place",
    "INEX_LD-2012323" : "Place",
    "INEX_LD-2012325" : "Person",
    "INEX_LD-2012327" : "Person",
    "INEX_LD-2012329" : "Other",
    "INEX_LD-2012331" : "Place",
    "INEX_LD-2012333" : "Person",
    "INEX_LD-2012335" : "Person",
    "INEX_LD-2012336" : "Place",
    "INEX_LD-2012337" : "Other",
    "INEX_LD-2012339" : "Person",
    "INEX_LD-2012341" : "Person",
    "INEX_LD-2012343" : "Other",
    "INEX_LD-2012345" : "Person",
    "INEX_LD-2012347" : "Place",
    "INEX_LD-2012349" : "Place",
    "INEX_LD-2012351" : "Other",
    "INEX_LD-2012353" : "Place",
    "INEX_LD-2012354" : "Person",
    "INEX_LD-2012355" : "Person",
    "INEX_LD-2012357" : "Person",
    "INEX_LD-2012359" : "Person",
    "INEX_LD-2012361" : "Person",
    "INEX_LD-2012363" : "Person",
    "INEX_LD-2012365" : "Person",
    "INEX_LD-2012367" : "Person",
    "INEX_LD-2012369" : "Person",
    "INEX_LD-2012371" : "Place",
    "INEX_LD-2012372" : "Place",
    "INEX_LD-2012373" : "Other",
    "INEX_LD-2012375" : "Other",
    "INEX_LD-2012377" : "Person",
    "INEX_LD-2012381" : "Person",
    "INEX_LD-2012383" : "Person",
    "INEX_LD-2012385" : "Person",
    "INEX_LD-2012387" : "Place",
    "INEX_LD-2012389" : "Place",
    "INEX_LD-2012390" : "Person",
    "INEX_XER-60" : "Person",
    "INEX_XER-62" : "Other",
    "INEX_XER-63" : "Other",
    "INEX_XER-64" : "Other",
    "INEX_XER-65" : "Person",
    "INEX_XER-67" : "Place",
    "INEX_XER-72" : "Other",
    "INEX_XER-73" : "Other",
    "INEX_XER-74" : "Other",
    "INEX_XER-79" : "Other",
    "INEX_XER-81" : "Other",
    "INEX_XER-86" : "Place",
    "INEX_XER-87" : "Place",
    "INEX_XER-88" : "Person",
    "INEX_XER-91" : "Other",
    "INEX_XER-94" : "Other",
    "INEX_XER-95" : "Other",
    "INEX_XER-96" : "Other",
    "INEX_XER-97" : "Other",
    "INEX_XER-98" : "Person",
    "INEX_XER-99" : "Other",
    "INEX_XER-100" : "Other",
    "INEX_XER-106" : "Person",
    "INEX_XER-108" : "Place",
    "INEX_XER-109" : "Place",
    "INEX_XER-110" : "Person",
    "INEX_XER-113" : "Person",
    "INEX_XER-114" : "Other",
    "INEX_XER-115" : "Person",
    "INEX_XER-116" : "Person",
    "INEX_XER-117" : "Person",
    "INEX_XER-118" : "Other",
    "INEX_XER-119" : "Place",
    "INEX_XER-121" : "Person",
    "INEX_XER-122" : "Other",
    "INEX_XER-123" : "Person",
    "INEX_XER-124" : "Person",
    "INEX_XER-125" : "Place",
    "INEX_XER-126" : "Organisation",
    "INEX_XER-127" : "Person",
    "INEX_XER-128" : "Person",
    "INEX_XER-129" : "Other",
    "INEX_XER-130" : "Person",
    "INEX_XER-132" : "Person",
    "INEX_XER-133" : "Other",
    "INEX_XER-134" : "Person",
    "INEX_XER-135" : "Organisation",
    "INEX_XER-136" : "Person",
    "INEX_XER-138" : "Place",
    "INEX_XER-139" : "Person",
    "INEX_XER-140" : "Place",
    "INEX_XER-141" : "Organisation",
    "INEX_XER-143" : "Organisation",
    "INEX_XER-144" : "Person",
    "INEX_XER-147" : "Other",
    "QALD2_te-1" : "Place",
    "QALD2_te-2" : "Person",
    "QALD2_te-3" : "Person",
    "QALD2_te-5" : "Place",
    "QALD2_te-6" : "Person",
    "QALD2_te-8" : "Place",
    "QALD2_te-9" : "Person",
    "QALD2_te-11" : "Person",
    "QALD2_te-12" : "Place",
    "QALD2_te-13" : "Person",
    "QALD2_te-14" : "Person",
    "QALD2_te-15" : "Place",
    "QALD2_te-17" : "Other",
    "QALD2_te-19" : "Person",
    "QALD2_te-21" : "Place",
    "QALD2_te-22" : "Person",
    "QALD2_te-24" : "Person",
    "QALD2_te-25" : "Place",
    "QALD2_te-27" : "Person",
    "QALD2_te-28" : "Other",
    "QALD2_te-29" : "Person",
    "QALD2_te-31" : "Person",
    "QALD2_te-33" : "Organisation",
    "QALD2_te-34" : "Person",
    "QALD2_te-35" : "Organisation",
    "QALD2_te-39" : "Organisation",
    "QALD2_te-40" : "Other",
    "QALD2_te-41" : "Person",
    "QALD2_te-42" : "Person",
    "QALD2_te-43" : "Other",
    "QALD2_te-44" : "Place",
    "QALD2_te-45" : "Place",
    "QALD2_te-46" : "Person",
    "QALD2_te-48" : "Place",
    "QALD2_te-49" : "Other",
    "QALD2_te-51" : "Place",
    "QALD2_te-53" : "Organisation",
    "QALD2_te-55" : "Person",
    "QALD2_te-57" : "Person",
    "QALD2_te-58" : "Other",
    "QALD2_te-59" : "Place",
    "QALD2_te-60" : "Place",
    "QALD2_te-63" : "Other",
    "QALD2_te-64" : "Place",
    "QALD2_te-65" : "Other",
    "QALD2_te-66" : "Other",
    "QALD2_te-67" : "Person",
    "QALD2_te-72" : "Place",
    "QALD2_te-75" : "Person",
    "QALD2_te-76" : "Person",
    "QALD2_te-77" : "Person",
    "QALD2_te-80" : "Other",
    "QALD2_te-81" : "Other",
    "QALD2_te-82" : "Other",
    "QALD2_te-84" : "Person",
    "QALD2_te-86" : "Place",
    "QALD2_te-87" : "Person",
    "QALD2_te-88" : "Other",
    "QALD2_te-89" : "Place",
    "QALD2_te-90" : "Place",
    "QALD2_te-91" : "Place",
    "QALD2_te-92" : "Other",
    "QALD2_te-93" : "Other",
    "QALD2_te-95" : "Person",
    "QALD2_te-97" : "Person",
    "QALD2_te-98" : "Place",
    "QALD2_te-99" : "Organisation",
    "QALD2_te-100" : "Organisation",
    "QALD2_tr-1" : "Person",
    "QALD2_tr-3" : "Person",
    "QALD2_tr-4" : "Place",
    "QALD2_tr-6" : "Place",
    "QALD2_tr-8" : "Place",
    "QALD2_tr-9" : "Place",
    "QALD2_tr-10" : "Place",
    "QALD2_tr-11" : "Place",
    "QALD2_tr-13" : "Other",
    "QALD2_tr-15" : "Person",
    "QALD2_tr-16" : "Place",
    "QALD2_tr-17" : "Place",
    "QALD2_tr-18" : "Place",
    "QALD2_tr-21" : "Place",
    "QALD2_tr-22" : "Place",
    "QALD2_tr-23" : "Other",
    "QALD2_tr-24" : "Place",
    "QALD2_tr-25" : "Other",
    "QALD2_tr-26" : "Place",
    "QALD2_tr-28" : "Place",
    "QALD2_tr-29" : "Other",
    "QALD2_tr-30" : "Place",
    "QALD2_tr-31" : "Other",
    "QALD2_tr-32" : "Place",
    "QALD2_tr-34" : "Place",
    "QALD2_tr-35" : "Person",
    "QALD2_tr-36" : "Place",
    "QALD2_tr-38" : "Person",
    "QALD2_tr-40" : "Place",
    "QALD2_tr-41" : "Organisation",
    "QALD2_tr-42" : "Other",
    "QALD2_tr-43" : "Person",
    "QALD2_tr-44" : "Person",
    "QALD2_tr-45" : "Organisation",
    "QALD2_tr-47" : "Place",
    "QALD2_tr-49" : "Organisation",
    "QALD2_tr-50" : "Other",
    "QALD2_tr-51" : "Other",
    "QALD2_tr-52" : "Person",
    "QALD2_tr-53" : "Person",
    "QALD2_tr-54" : "Person",
    "QALD2_tr-55" : "Organisation",
    "QALD2_tr-57" : "Other",
    "QALD2_tr-58" : "Person",
    "QALD2_tr-59" : "Person",
    "QALD2_tr-61" : "Place",
    "QALD2_tr-62" : "Person",
    "QALD2_tr-63" : "Person",
    "QALD2_tr-64" : "Other",
    "QALD2_tr-65" : "Organisation",
    "QALD2_tr-68" : "Person",
    "QALD2_tr-69" : "Place",
    "QALD2_tr-70" : "Other",
    "QALD2_tr-71" : "Other",
    "QALD2_tr-72" : "Other",
    "QALD2_tr-73" : "Person",
    "QALD2_tr-74" : "Place",
    "QALD2_tr-75" : "Person",
    "QALD2_tr-77" : "Other",
    "QALD2_tr-78" : "Other",
    "QALD2_tr-79" : "Place",
    "QALD2_tr-80" : "Organisation",
    "QALD2_tr-81" : "Place",
    "QALD2_tr-82" : "Other",
    "QALD2_tr-83" : "Person",
    "QALD2_tr-84" : "Person",
    "QALD2_tr-85" : "Other",
    "QALD2_tr-86" : "Person",
    "QALD2_tr-87" : "Person",
    "QALD2_tr-89" : "Organisation",
    "QALD2_tr-91" : "Organisation",
    "QALD2_tr-92" : "Place",
    "SemSearch_ES-1" : "Other",
    "SemSearch_ES-2" : "Person",
    "SemSearch_ES-3" : "Other",
    "SemSearch_ES-4" : "Other",
    "SemSearch_ES-5" : "Place",
    "SemSearch_ES-6" : "Other",
    "SemSearch_ES-7" : "Other",
    "SemSearch_ES-9" : "Place",
    "SemSearch_ES-10" : "Place",
    "SemSearch_ES-11" : "Other",
    "SemSearch_ES-12" : "Place",
    "SemSearch_ES-13" : "Other",
    "SemSearch_ES-14" : "Person",
    "SemSearch_ES-15" : "Other",
    "SemSearch_ES-16" : "Place",
    "SemSearch_ES-17" : "Place",
    "SemSearch_ES-18" : "Other",
    "SemSearch_ES-19" : "Person",
    "SemSearch_ES-20" : "Other",
    "SemSearch_ES-21" : "Person",
    "SemSearch_ES-22" : "Place",
    "SemSearch_ES-23" : "Place",
    "SemSearch_ES-24" : "Place",
    "SemSearch_ES-25" : "Person",
    "SemSearch_ES-26" : "Place",
    "SemSearch_ES-28" : "Place",
    "SemSearch_ES-29" : "Place",
    "SemSearch_ES-30" : "Other",
    "SemSearch_ES-31" : "Other",
    "SemSearch_ES-32" : "Place",
    "SemSearch_ES-33" : "Other",
    "SemSearch_ES-34" : "Other",
    "SemSearch_ES-36" : "Other",
    "SemSearch_ES-37" : "Person",
    "SemSearch_ES-38" : "Person",
    "SemSearch_ES-39" : "Place",
    "SemSearch_ES-40" : "Person",
    "SemSearch_ES-41" : "Person",
    "SemSearch_ES-42" : "Person",
    "SemSearch_ES-45" : "Person",
    "SemSearch_ES-47" : "Person",
    "SemSearch_ES-49" : "Person",
    "SemSearch_ES-50" : "Person",
    "SemSearch_ES-52" : "Place",
    "SemSearch_ES-53" : "Place",
    "SemSearch_ES-54" : "Person",
    "SemSearch_ES-56" : "Other",
    "SemSearch_ES-57" : "Person",
    "SemSearch_ES-58" : "Place",
    "SemSearch_ES-59" : "Place",
    "SemSearch_ES-60" : "Person",
    "SemSearch_ES-61" : "Place",
    "SemSearch_ES-63" : "Other",
    "SemSearch_ES-65" : "Place",
    "SemSearch_ES-66" : "Other",
    "SemSearch_ES-67" : "Other",
    "SemSearch_ES-68" : "Place",
    "SemSearch_ES-70" : "Other",
    "SemSearch_ES-71" : "Place",
    "SemSearch_ES-72" : "Other",
    "SemSearch_ES-73" : "Place",
    "SemSearch_ES-74" : "Other",
    "SemSearch_ES-75" : "Place",
    "SemSearch_ES-76" : "Place",
    "SemSearch_ES-77" : "Place",
    "SemSearch_ES-78" : "Other",
    "SemSearch_ES-79" : "Other",
    "SemSearch_ES-80" : "Other",
    "SemSearch_ES-81" : "Place",
    "SemSearch_ES-82" : "Other",
    "SemSearch_ES-83" : "Other",
    "SemSearch_ES-84" : "Other",
    "SemSearch_ES-85" : "Other",
    "SemSearch_ES-86" : "Place",
    "SemSearch_ES-88" : "Person",
    "SemSearch_ES-89" : "Place",
    "SemSearch_ES-90" : "Place",
    "SemSearch_ES-91" : "Place",
    "SemSearch_ES-93" : "Other",
    "SemSearch_ES-94" : "Person",
    "SemSearch_ES-95" : "Place",
    "SemSearch_ES-96" : "Other",
    "SemSearch_ES-97" : "Other",
    "SemSearch_ES-98" : "Place",
    "SemSearch_ES-99" : "Place",
    "SemSearch_ES-100" : "Other",
    "SemSearch_ES-101" : "Person",
    "SemSearch_ES-102" : "Place",
    "SemSearch_ES-104" : "Place",
    "SemSearch_ES-106" : "Person",
    "SemSearch_ES-107" : "Other",
    "SemSearch_ES-108" : "Person",
    "SemSearch_ES-109" : "Person",
    "SemSearch_ES-111" : "Place",
    "SemSearch_ES-114" : "Person",
    "SemSearch_ES-115" : "Other",
    "SemSearch_ES-118" : "Organisation",
    "SemSearch_ES-119" : "Person",
    "SemSearch_ES-120" : "Place",
    "SemSearch_ES-123" : "Person",
    "SemSearch_ES-124" : "Other",
    "SemSearch_ES-125" : "Other",
    "SemSearch_ES-127" : "Other",
    "SemSearch_ES-128" : "Other",
    "SemSearch_ES-129" : "Place",
    "SemSearch_ES-130" : "Place",
    "SemSearch_ES-131" : "Place",
    "SemSearch_ES-132" : "Organisation",
    "SemSearch_ES-135" : "Other",
    "SemSearch_ES-136" : "Organisation",
    "SemSearch_ES-137" : "Other",
    "SemSearch_ES-139" : "Place",
    "SemSearch_ES-141" : "Place",
    "SemSearch_LS-1" : "Person",
    "SemSearch_LS-2" : "Place",
    "SemSearch_LS-3" : "Person",
    "SemSearch_LS-4" : "Place",
    "SemSearch_LS-5" : "Other",
    "SemSearch_LS-6" : "Place",
    "SemSearch_LS-7" : "Organisation",
    "SemSearch_LS-8" : "Place",
    "SemSearch_LS-9" : "Other",
    "SemSearch_LS-10" : "Other",
    "SemSearch_LS-11" : "Organisation",
    "SemSearch_LS-12" : "Place",
    "SemSearch_LS-13" : "Other",
    "SemSearch_LS-14" : "Person",
    "SemSearch_LS-16" : "Person",
    "SemSearch_LS-17" : "Organisation",
    "SemSearch_LS-18" : "Person",
    "SemSearch_LS-19" : "Person",
    "SemSearch_LS-20" : "Person",
    "SemSearch_LS-21" : "Person",
    "SemSearch_LS-22" : "Organisation",
    "SemSearch_LS-24" : "Person",
    "SemSearch_LS-25" : "Person",
    "SemSearch_LS-26" : "Other",
    "SemSearch_LS-29" : "Place",
    "SemSearch_LS-30" : "Other",
    "SemSearch_LS-31" : "Place",
    "SemSearch_LS-32" : "Person",
    "SemSearch_LS-33" : "Place",
    "SemSearch_LS-34" : "Other",
    "SemSearch_LS-35" : "Place",
    "SemSearch_LS-36" : "Person",
    "SemSearch_LS-37" : "Other",
    "SemSearch_LS-38" : "Place",
    "SemSearch_LS-39" : "Place",
    "SemSearch_LS-40" : "Place",
    "SemSearch_LS-41" : "Person",
    "SemSearch_LS-42" : "Other",
    "SemSearch_LS-43" : "Other",
    "SemSearch_LS-44" : "Other",
    "SemSearch_LS-46" : "Place",
    "SemSearch_LS-49" : "Person",
    "SemSearch_LS-50" : "Place",
    "TREC_Entity-1" : "Organisation",
    "TREC_Entity-2" : "Person",
    "TREC_Entity-4" : "Organisation",
    "TREC_Entity-5" : "Other",
    "TREC_Entity-6" : "Organisation",
    "TREC_Entity-7" : "Organisation",
    "TREC_Entity-9" : "Person",
    "TREC_Entity-10" : "Organisation",
    "TREC_Entity-11" : "Organisation",
    "TREC_Entity-12" : "Organisation",
    "TREC_Entity-14" : "Person",
    "TREC_Entity-15" : "Organisation",
    "TREC_Entity-16" : "Organisation",
    "TREC_Entity-17" : "Person",
    "TREC_Entity-18" : "Person",
    "TREC_Entity-19" : "Organisation",
    "TREC_Entity-20" : "Organisation"
    }



prefix_dict = {}
for line in trec_output.splitlines():
    line = line.decode("utf-8")
    if line.startswith("ndcg_cut_100"):
        query_id, ndcg10 = line.split()[1], line.split()[2]
        prefix = query_id.rsplit('-', 1)[0]
                
        if query_id == 'all':
            prefix_dict[query_id] = {}
            prefix_dict[query_id][query_id] = float(ndcg10)
            continue
        
        query_entity_type = dbpedia_entity_types[query_id]
        
        if query_entity_type not in prefix_dict:
            prefix_dict[query_entity_type] = {}
        
        prefix_dict[query_entity_type][query_id] = float(ndcg10)

for (prefix, ndcgs) in prefix_dict.items():
    if prefix == TYPE:
        avg_ndcg = sum(ndcgs.values()) / len(ndcgs)
        print(f"{avg_ndcg:.4f}")
