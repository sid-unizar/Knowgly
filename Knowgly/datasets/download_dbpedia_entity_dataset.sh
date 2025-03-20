#!/bin/bash

mkdir dbpedia-entity
# The original files list can be found in https://iai-group.github.io/DBpedia-Entity/index_details.html
# We removed:
# - Wikipedia page links, as we don't parse them
# - Transitive redirects (e.g <http://dbpedia.org/resource/Albania_History> <http://dbpedia.org/ontology/wikiPageRedirects> <http://dbpedia.org/resource/History_of_Albania>),
#   as the redirecting entities (<http://dbpedia.org/resource/Albania_History>) do not have any other information

#wget http://downloads.dbpedia.org/2015-10/core-i18n/en/transitive_redirects_en.ttl.bz2
wget http://downloads.dbpedia.org/2015-10/core-i18n/en/short_abstracts_en.ttl.bz2 -O dbpedia-entity/short_abstracts_en.ttl.bz2
wget http://downloads.dbpedia.org/2015-10/core-i18n/en/persondata_en.ttl.bz2 -O dbpedia-entity/persondata_en.ttl.bz2
#wget http://downloads.dbpedia.org/2015-10/core-i18n/en/page_links_en.ttl.bz2
wget http://downloads.dbpedia.org/2015-10/core-i18n/en/mappingbased_objects_en.ttl.bz2 -O dbpedia-entity/mappingbased_objects_en.ttl.bz2
wget http://downloads.dbpedia.org/2015-10/core-i18n/en/mappingbased_literals_en.ttl.bz2 -O dbpedia-entity/mappingbased_literals_en.ttl.bz2
wget http://downloads.dbpedia.org/2015-10/core-i18n/en/long_abstracts_en.ttl.bz2 -O dbpedia-entity/long_abstracts_en.ttl.bz2
wget http://downloads.dbpedia.org/2015-10/core-i18n/en/labels_en.ttl.bz2 -O dbpedia-entity/labels_en.ttl.bz2
wget http://downloads.dbpedia.org/2015-10/core-i18n/en/instance_types_transitive_en.ttl.bz2 -O dbpedia-entity/instance_types_transitive_en.ttl.bz2
wget http://downloads.dbpedia.org/2015-10/core-i18n/en/infobox_properties_en.ttl.bz2 -O dbpedia-entity/infobox_properties_en.ttl.bz2
wget http://downloads.dbpedia.org/2015-10/core-i18n/en/disambiguations_en.ttl.bz2 -O dbpedia-entity/disambiguations_en.ttl.bz2
wget http://downloads.dbpedia.org/2015-10/core-i18n/en/article_categories_en.ttl.bz2 -O dbpedia-entity/article_categories_en.ttl.bz2
wget http://downloads.dbpedia.org/2015-10/core-i18n/en/anchor_text_en.ttl.bz2 -O dbpedia-entity/anchor_text_en.ttl.bz2
