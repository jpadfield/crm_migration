def sparql_query():
    #key = key.replace(" ","_")
    #value = value.replace(" ","_")

    sparql = SPARQLWrapper('https://round4.ng-london.org.uk/ex/lcd')
    sparql.setQuery("""
        PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>
        PREFIX owl:<http://www.w3.org/2002/07/owl#>
        PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX crm:<http://www.cidoc-crm.org/rdfs/cidoc_crm_v5.0.2_english_label.rdfs#>
        PREFIX rro:<https://rdf.ng-london.org.uk/raphael/ontology/>
        PREFIX rri:<https://rdf.ng-london.org.uk/raphael/resource/>

        SELECT 
            DISTINCT ?s
        WHERE { ?s ?p ?o }
        """)
    sparql.setReturnFormat(JSON)
    #results = sparql.query().convert()
    
    ret = sparql.query()
    ret_csv = ret.convert()
    print(ret_csv)

def sparql_query_pythonic(subject,csv_format=True):
    qres = g.query(
        """
        PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>
        PREFIX owl:<http://www.w3.org/2002/07/owl#>
        PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX crm:<http://www.cidoc-crm.org/rdfs/cidoc_crm_v5.0.2_english_label.rdfs#>
        PREFIX rro:<https://rdf.ng-london.org.uk/raphael/ontology/>
        PREFIX rri:<https://rdf.ng-london.org.uk/raphael/resource/>

        SELECT 
        DISTINCT ?s ?p ?o
        WHERE { ?s rdf:type ?o }
        """
    )

    if csv_format == True: 
        new_dataframe = []
        for row in qres:
            triples = '%s, %s, %s' % row
            innerlist = []
            if 1 == 1:
                innerlist = triples.split(',')
            new_dataframe.append(innerlist)
        new_dataframe = [x for x in new_dataframe if x]
        return new_dataframe
    else:
        return qres


def aat_endpoint_query(literal):
    sparql = SPARQLWrapper('http://vocab.getty.edu/')

    query = ('''
        select ?Subject ?Term ?Parents ?Descr ?ScopeNote ?Type (coalesce(?Type1,?Type2) as ?ExtraType) {

        ?Subject luc:term "fishing* AND vessel*"; a ?typ.

        ?typ rdfs:subClassOf gvp:Subject; rdfs:label ?Type.

        filter (?typ != gvp:Subject)

        optional {?Subject gvp:placeTypePreferred [gvp:prefLabelGVP [xl:literalForm ?Type1]]}

        optional {?Subject gvp:agentTypePreferred [gvp:prefLabelGVP [xl:literalForm ?Type2]]}

        optional {?Subject gvp:prefLabelGVP [xl:literalForm ?Term]}

        optional {?Subject gvp:parentStringAbbrev ?Parents}

        optional {?Subject foaf:focus/gvp:biographyPreferred/schema:description ?Descr}

        optional {?Subject skos:scopeNote [dct:language gvp_lang:en; rdf:value ?ScopeNote]}}
        '''
    )

    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)

    ret = sparql.query().convert()

    for result in ret["results"]["bindings"]:
        print(result)


    else:
        es = Elasticsearch([{'host':'localhost','port':9200}])  

        body = {
                'column1' : input_literal,
                'column2' : placeholder_PID
                }
        es.index(index = 'pids',doc_type='_doc',body=body)
        print('new thing added to elasticsearch')