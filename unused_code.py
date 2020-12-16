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