from rdflib import Graph, Namespace, Literal, BNode
from rdflib.namespace import OWL, RDF, RDFS, NamespaceManager, XSD
from rdflib.serializer import Serializer
from SPARQLWrapper import SPARQLWrapper, JSON
import csv, random, string, json, urllib
import os.path
from openpyxl import load_workbook
from elasticsearch import Elasticsearch
from alive_progress import alive_bar
import requests
import time
import mysql.connector

from pdb import set_trace as st
#exit this with c

RRO = Namespace("https://rdf.ng-london.org.uk/raphael/ontology/")
RRI = Namespace("https://rdf.ng-london.org.uk/raphael/resource/")
CRM = Namespace("http://www.cidoc-crm.org/cidoc-crm/")
NGO = Namespace("https://round4.ng-london.org.uk/ex/lcd/")
AAT = Namespace("http://vocab.getty.edu/page/aat/")
TGN = Namespace("http://vocab.getty.edu/page/tgn/")
WD = Namespace("http://www.wikidata.org/entity/")

g = Graph()
g.parse("inputs/rrr_i_v0.5.xml", format="xml")
g.bind('rro',RRO)
g.bind('rri',RRI)

new_graph = Graph()
new_graph.namespace_manager.bind('crm',CRM)
new_graph.namespace_manager.bind('ngo',NGO)
new_graph.namespace_manager.bind('aat',AAT)
new_graph.namespace_manager.bind('tgn',TGN)
new_graph.namespace_manager.bind('wd',WD)
new_graph.namespace_manager.bind('rro',RRO)
new_graph.namespace_manager.bind('rri',RRI)

#print(len(g)) # prints 2

#print(RDF.type)
#print(RDFS.subClassOf)
#print(getattr(RRO,'RP16.has_height_in_cm'))

def query_graph(graph,subj,pred,obj):
    for s,p,o in graph.triples((subj,pred,obj)):
        print(s.n3(new_graph.namespace_manager),p.n3(new_graph.namespace_manager),o.n3(new_graph.namespace_manager))

def map_property(graph,new_graph, old_property, new_property):
    for x, old_property, z in graph.triples((None, old_property, None)):
        #graph.remove((x,y,z))
        new_graph.add((x, new_property, z))
    return graph, new_graph

def map_class(graph,new_graph,old_class,new_class):
    for x,y,z in graph.subjects((old_class,None,None)):
        #graph.remove((x,y,z))
        new_graph.add((new_class,y,z))

    for x,y,z in graph.objects((None,None,old_class)):
        #graph.remove((x,y,z))
        new_graph.add((x,y,new_class))

    return graph, new_graph

def triples_to_csv(triples, filename):
    fields = ['Subject','Predicate','Object']

    with open('outputs/' + filename + '.csv','w',newline='') as f:
        write = csv.writer(f)

        write.writerow(fields)
        write.writerows(triples)

    print('CSV created!')

def triples_to_tsv(triples, filename):
    fields = ['Subject','Predicate','Object']

    with open('outputs/' + filename + '.tsv','w',newline='') as f:
        write = csv.writer(f, delimiter = '\t')

        write.writerow(fields)
        write.writerows(triples)

    print('TSV created!')

def connect_to_sql():
    mydb = mysql.connector.connect(
        host="round4",
        user="sshoc",
        password="IqazZuKaXMmSEqUl"
    )

    return mydb

def get_property(uri):
    remove_uri = uri.replace('https://rdf.ng-london.org.uk/raphael/resource/','')
    final_property = remove_uri.replace('_',' ')
    if '.' in final_property:
        final_property = str(final_property.split('.')[1])
    if 'RRR' in final_property:
        final_property = final_property.replace('RRR','')
    return final_property

def get_json(url):
    operUrl = urllib.request.urlopen(url)
    if(operUrl.getcode()==200):
       data = operUrl.read()
       json_data = json.loads(data)
    else:
       print("Error receiving data", operUrl.getcode())
    return json_data

def check_pids_csv(input_literal):
    pid_value = 'None'
    if os.path.isfile('outputs/placeholder_pids.csv') == True:
        with open('outputs/placeholder_pids.csv','r') as f:
            reader = csv.DictReader(f, delimiter=',')
            dict_list = []
            for row in reader:
                dict_list.append(row)
            for value in range(len(dict_list)):
                csv_literal = str(dict_list[value]['Literal value'])
                if csv_literal == str(input_literal):
                    pid_value = str(dict_list[value]['Placeholder PID'])
                    break
    return pid_value

def check_db(input_literal, table_name):
    db = connect_to_sql()
    cursor = db.cursor()
    pid_value = None
    input_literal = str(input_literal)
    if table_name == 'temp_pids':
        val = 'pid_value'
        str_input = 'literal_value'
    elif table_name == 'wikidata':
        val = 'string_literal'
        str_input = 'wd_value'

    query = "SELECT " + val + " FROM sshoc." + table_name + " WHERE " + str_input + " = '" + input_literal + "'"
    cursor.execute(query)
    result = cursor.fetchall()
    
    if len(result) > 0:
        pid_value = result[0][0]

    return pid_value

def check_elasticsearch(search_term, index_name): 
    es = Elasticsearch([{'host':'localhost','port':9200}]) 
    search_term = str(search_term)

    pid_value = None
    body_query = {'query':{'match':{'column1':{'query':search_term, 'fuzziness':0}}}}
    res = es.search(index=index_name,body=body_query)

    for hit in res['hits']['hits']:
        pid_value = hit['_source']['column2']
    
    return pid_value

def find_old_pid(ng_number):
    #json_data = get_json('https://scientific.ng-london.org.uk/export/ngpidexport_00A.json')
    if ng_number.startswith('https'):
        ng_number = str(ng_number.rsplit('/',1)[-1])
    else:
        ng_number
    old_pid = None

    with open('ngpidexports/ngpidexport_00A.json') as f:
        json_data = json.load(f)
        for x in json_data:
            for y in json_data[x]["objects"]:
                if json_data[x]["objects"][y] == ng_number:
                    old_pid = y
                    break
    return old_pid

def generate_placeholder_PID(input_literal):
    N = 4
    placeholder_PID = ""
    for number in range(N):
        res = ''.join(random.choices(string.ascii_uppercase + string.digits, k = N))
        placeholder_PID += str(res)
        placeholder_PID += '-'
    placeholder_PID = placeholder_PID[:-1]
    #input_list = [input_literal, placeholder_PID]
    #fields = ['Literal value','Placeholder PID']
    existing_pid = check_db(input_literal, table_name = 'temp_pids')
    print(existing_pid)
    old_pid = find_old_pid(input_literal)
    
    if existing_pid is not None:
        return existing_pid
    elif old_pid is not None:
        return old_pid
    else:
        db = connect_to_sql()
        cursor = db.cursor()
        input_literal = str(input_literal)
        query = 'INSERT INTO sshoc.temp_pids (literal_value, pid_value) VALUES (%s, %s)'
        val = (input_literal, placeholder_PID)
        cursor.execute(query, val)
        db.commit()

        placeholder_PID = str(placeholder_PID)
        return placeholder_PID

def create_PID_from_triple(pid_type, subj):
    if pid_type == 'object':
        pid_name = subj
    else:
        pid_name = pid_type + ' of ' + subj
    output_pid = generate_placeholder_PID(pid_name)

    return output_pid

def find_aat_value(material,material_type):
    material = str(get_property(material))
    if material_type == getattr(RRO,'RP20.has_medium'):
        wb = load_workbook(filename = 'inputs/NG_Meduim_and_Support_AAT.xlsx', read_only=True)
        ws = wb['Medium Material']
    elif material_type == getattr(RRO,'RP32.has_support'):
        wb = load_workbook(filename = 'inputs/NG_Meduim_and_Support_AAT.xlsx', read_only=True)
        ws = wb['Support Materials']
    elif material_type == getattr(RRO, 'RP215.has_acted_in_the_role_of_an'):
        wb = load_workbook(filename = 'inputs/NG_Roles_AAT.xlsx', read_only=True)
        ws = wb['AAT_Roles']
    for row in ws.iter_rows(values_only=True):
        if material in row:
            aat_number_string = str(row[1])
            aat_number = aat_number_string.replace('aat:','')
            aat_type = row[2]
            return aat_number, aat_type
    wb.close()

def wikidata_query(literal, literal_type):
    sparql = SPARQLWrapper('https://query.wikidata.org/sparql')
    print('Trying a query with input ' + literal)
    remove_uri = literal.replace('https://rdf.ng-london.org.uk/raphael/resource/','')
    literal = remove_uri.replace('_',' ')
    literal = literal.replace('%C3%A9', 'e')
    literal = literal.replace('%C3%A0', 'a')


    if literal_type == 'year':
        thing_type = 'Q577'
        if 'RRR' in literal:
            string_literal = str(literal.rsplit(' ')[-1])
        elif 'About' in literal:
            string_literal = str(literal.rsplit('-')[-1])
        else:
            return None
    elif literal_type == 'institution':
        thing_type = 'Q207694'
        string_literal = str(literal)
    elif literal_type == 'location':
        thing_type = '?dunno'
        string_literal = str(literal)
    
    result = check_db(string_literal, 'wikidata')
    if result is not None:
        return result
    else:
        query = ('''
        SELECT DISTINCT ?year
        WHERE
        {
        ?year  wdt:P31 wd:''' + thing_type + ''' .
                ?year rdfs:label ?yearLabel .
        FILTER( str(?yearLabel) = "''' + string_literal + '''" ) .
        SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
        }
        ''')

        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)

        ret = sparql.query().convert()
        time.sleep(3)

        for result in ret["results"]["bindings"]:
            db = connect_to_sql()
            cursor = db.cursor()
            result = result["year"]["value"]
            query = 'INSERT INTO sshoc.wikidata (string_literal, wd_value) VALUES (%s, %s)'
            val = (string_literal, result)
            cursor.execute(query, val)
            db.commit()

            return result

def run_ruby_program(input_string):
    import subprocess

    ruby_var = 'ruby citation_parser.rb \'' + input_string + '\''
    output = subprocess.Popen(ruby_var, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, error = output.communicate()

    string_output = out.decode("utf-8")
    json_output = json.loads(string_output)

    return json_output

def create_year_dates(year):
    import datetime
    year = int(year)

    start = datetime.datetime(year, 1, 1)
    end = datetime.datetime(year, 12, 31)

    return start, end

def create_title_triples(PID, subj, pred, obj):
    if pred == getattr(RRO, 'RP34.has_title'):
        title_PID = BNode()

        new_graph.add((getattr(NGO,PID), CRM.P102_has_title, title_PID))
        new_graph.add((title_PID, CRM.P1i_identifies, Literal(obj)))
        new_graph.add((title_PID, CRM.P2_has_type, CRM.E35_Title))
        new_graph.add((title_PID, CRM.P2_has_type, getattr(AAT,'300417209')))
        new_graph.add((getattr(AAT,'300417209'), CRM.P1i_identifies, Literal('full title@en')))

    elif pred == getattr(RRO, 'RP31.has_short_title'):
        title_PID = BNode()

        new_graph.add((getattr(NGO,PID), CRM.P102_has_title, title_PID))
        new_graph.add((title_PID, CRM.P1i_identifies, Literal(obj)))
        new_graph.add((title_PID, CRM.P2_has_type, CRM.E35_Title))
        new_graph.add((title_PID, CRM.P2_has_type, getattr(AAT,'300417208')))
        new_graph.add((getattr(AAT,'300417208'), CRM.P1i_identifies, Literal('brief title@en')))

    return new_graph

def create_medium_triples(subject_PID, subj, pred, obj):
    if pred == getattr(RRO, 'RP20.has_medium'):
        medium_PID = BNode()
        aat_number, aat_type = find_aat_value(obj, pred)

        new_graph.add((getattr(NGO,subject_PID), CRM.P45_consists_of, medium_PID))
        new_graph.add((medium_PID, CRM.P2_has_type, getattr(AAT,aat_number)))
        new_graph.add(((getattr(AAT,aat_number)), CRM.P1i_identifies, Literal(aat_type)))
        new_graph.add((medium_PID, CRM.P2_has_type, CRM.E57_Material))
        new_graph.add((medium_PID, CRM.P2_has_type, getattr(AAT,'300163343')))
        new_graph.add(((getattr(AAT,'300163343')), CRM.P1i_identifies, Literal('media (artists\' material)@en')))

    elif pred == getattr(RRO, 'RP32.has_support'):
        medium_PID = BNode()
        aat_number, aat_type = find_aat_value(obj, pred)

        new_graph.add((getattr(NGO,subject_PID), CRM.P45_consists_of, medium_PID))
        new_graph.add((medium_PID, CRM.P2_has_type, getattr(AAT,aat_number)))
        new_graph.add(((getattr(AAT,aat_number)), CRM.P1i_identifies, Literal(aat_type)))
        new_graph.add((medium_PID, CRM.P2_has_type, CRM.E57_Material))
        new_graph.add((medium_PID, CRM.P2_has_type, getattr(AAT,'300014844')))
        new_graph.add(((getattr(AAT,'300014844')), CRM.P1i_identifies, Literal('supports (artists\' materials)@en')))

    return new_graph

def create_collection_triples(subject_PID, subj, pred, obj):
    if pred == getattr(RRO, 'RP99.is_part_of'):
        collection_PID = create_PID_from_triple('object', obj)

        new_graph.add((getattr(NGO,subject_PID), CRM.P46_forms_part_of, getattr(NGO, collection_PID)))
        new_graph.add((getattr(NGO, collection_PID), CRM.P2_has_type, CRM.E78_Curated_Holding))
        new_graph.add((getattr(NGO, collection_PID), CRM.P1i_identifies, Literal(obj)))

    return new_graph

def create_dimension_triples(subject_PID, subj, pred, obj):
    if pred == getattr(RRO, 'RP36.has_width_in_cm'):
        dimension_PID = BNode()

        new_graph.add((getattr(NGO,subject_PID), CRM.P43_has_dimension, dimension_PID))
        new_graph.add((dimension_PID, CRM.P2_has_type, CRM.E54_Dimension))
        new_graph.add((dimension_PID, CRM.P90_has_value, Literal(obj, datatype=XSD.double)))
        new_graph.add((dimension_PID, CRM.P91_has_unit, getattr(AAT,'300379098')))
        new_graph.add((getattr(AAT,'300379098'), CRM.P1i_identifies, Literal('centimeters@en')))
        new_graph.add((dimension_PID, CRM.P2_has_type, getattr(AAT,'300055647')))
        new_graph.add((getattr(AAT,'300055647'), CRM.P1i_identifies, Literal('width@en')))

    elif pred == getattr(RRO, 'RP16.has_height_in_cm'): 
        dimension_PID = BNode()

        new_graph.add((getattr(NGO,subject_PID), CRM.P43_has_dimension, dimension_PID))
        new_graph.add((dimension_PID, CRM.P2_has_type, CRM.E54_Dimension))
        new_graph.add((dimension_PID, CRM.P90_has_value, Literal(obj, datatype=XSD.double)))
        new_graph.add((dimension_PID, CRM.P91_has_unit, getattr(AAT,'300379098')))
        new_graph.add((getattr(AAT,'300379098'), CRM.P1i_identifies, Literal('centimeters@en')))
        new_graph.add((dimension_PID, CRM.P2_has_type, getattr(AAT,'300055644')))
        new_graph.add((getattr(AAT,'300055644'), CRM.P1i_identifies, Literal('height@en')))

    return new_graph

def create_identifier_triples(subject_PID, pred, obj):
    if pred == getattr(RRO, 'RP17.has_identifier'):
        blank_node = BNode()
        new_graph.add((getattr(NGO, subject_PID), CRM.P48_has_preferred_identifier, blank_node))
        new_graph.add((blank_node, CRM.P2_has_type, CRM.E42_Identifier))
        #new_graph.add((blank_node, CRM.P2_has_type, getattr(AAT, '300312355')))
        #new_graph.add((getattr(AAT, '300312355'), CRM.P1i_identifies, Literal('accession numbers@en')))
        new_graph.add((blank_node, CRM.P1i_identifies, Literal(obj)))

    return new_graph

def create_type_triples(subject_PID, pred, obj):
    if pred == getattr(RRO, 'RP98.is_in_project_category'):
        new_graph.add((getattr(NGO, subject_PID), CRM.P2_has_type, obj))
    elif pred == RDF.type and obj == getattr(RRO, 'RC12.Painting'):
        new_graph.add((getattr(NGO, subject_PID), CRM.P2_has_type, CRM.E22_Human_Made_Object))
    elif pred == RDF.type and obj == getattr(RRO, 'RC40.Person'):
        new_graph.add((getattr(NGO, subject_PID), CRM.P2_has_type, CRM.E21_Person))
    elif pred == RDF.type and obj == getattr(RRO, 'RC41.Institution'):
        new_graph.add((getattr(NGO, subject_PID), CRM.P2_has_type, CRM.E74_Group))
    elif pred == RDF.type and obj == getattr(RRO, 'RC10.Building'):
        new_graph.add((getattr(NGO, subject_PID), CRM.P2_has_type, CRM.E53_Place))
    elif pred == RDF.type and obj == getattr(RRO, 'RC26.Digital_Document'):
        new_graph.add((getattr(NGO, subject_PID), CRM.P2_has_type, CRM.E31_Document))

    return new_graph

def create_time_span_triples(subject_PID, subj, pred, obj):
    if pred == getattr(RRO, 'RP209.has_time-span'):
        time_span_PID = BNode()

        #will need to change this logic if we encounter a time span that isn't a year
        new_graph.add((getattr(NGO, subject_PID), CRM.P4_has_time_span, time_span_PID))
        new_graph.add((time_span_PID, CRM.P2_has_type, CRM.E52_Time_span))
        new_graph.add((time_span_PID, CRM.P2_has_type, getattr(AAT, '300379244')))
        new_graph.add((getattr(AAT, '300379244'), CRM.P1i_identifies, Literal('years@en')))
        new_graph.add((time_span_PID, CRM.P1i_identifies, Literal(obj)))

        obj_year = get_property(obj)
        api_link = 'https://scientific.ng-london.org.uk/api/api-tms-v.2.3.0.php?format=json&what=timespan&which=' + obj_year
        r = requests.get(api_link)
        response_json = r.json()
        start_date = response_json[1][0]
        end_date = response_json[1][1]
        
        wikidata_year = wikidata_query(obj_year, 'year')
        new_graph.add((time_span_PID, CRM.P82a_begin_of_the_begin, Literal(start_date, datatype=XSD.dateTime)))
        new_graph.add((time_span_PID, CRM.P82b_end_of_the_end, Literal(end_date, datatype=XSD.dateTime)))
        if wikidata_year != None:
            new_graph.add((time_span_PID, OWL.sameAs, getattr(WD, wikidata_year)))
        
    return new_graph

def create_event_triples(subject_PID, obj_PID, subj, pred):
    if pred == getattr(RRO, 'RP68.was_acquired'):
        event_type = CRM.E8_Acquisition
        event_property = CRM.P24i_changed_ownership_through
        aat_event_id = getattr(AAT, '300157782')
        aat_event_type = Literal('acquisition (collections management)@en')
    elif pred == getattr(RRO, 'RP72.was_produced'):
        event_type = CRM.E12_Production
        event_property = CRM.P108i_was_produced_by
        aat_event_id = getattr(AAT, '300404387')
        aat_event_type = Literal('creating (artistic activity)@en')
    elif pred == getattr(RRO, 'RP42.was_born_in'):
        event_type = CRM.E67_Birth
        event_property = CRM.P98i_was_born
        aat_event_id = getattr(AAT, '300069672')
        aat_event_type = Literal('births@en')
    elif pred == getattr(RRO, 'RP4.died_in'):
        event_type = CRM.E69_Death
        event_property = CRM.P100i_died_in
        aat_event_id = getattr(AAT, '300151836')
        aat_event_type = Literal('deaths@en')

    new_graph.add((getattr(NGO, subject_PID), event_property, getattr(NGO, obj_PID)))
    new_graph.add((getattr(NGO, obj_PID), CRM.P2_has_type, event_type))
    new_graph.add((getattr(NGO, obj_PID), CRM.P2_has_type, aat_event_id))
    new_graph.add((aat_event_id, CRM.P1i_identifies, aat_event_type))

    return new_graph

def create_name_triples(subject_PID, subj, pred, obj):
    if pred == getattr(RRO, 'RP56.has_name'):
        name_PID = BNode()
        new_graph.add((getattr(NGO, subject_PID), CRM.P1_is_identified_by, name_PID))
        new_graph.add((name_PID, CRM.P2_has_type, CRM.E41_Appellation))
        new_graph.add((name_PID, CRM.P1i_identifies, Literal(obj)))
    elif pred == getattr(RRO, 'RP11.has_current_position'):
        job_title_PID = BNode()
        new_graph.add((getattr(NGO, subject_PID), CRM.P1_is_identified_by, job_title_PID))
        new_graph.add((job_title_PID, CRM.P2_has_type, CRM.E41_Appellation))
        new_graph.add((job_title_PID, CRM.P1i_identifies, Literal(obj)))

    return new_graph

def create_role_triples(subject_PID, pred, obj):
    if pred == getattr(RRO, 'RP215.has_acted_in_the_role_of_an'):
        aat_number, aat_type = find_aat_value(obj, pred)

        new_graph.add((getattr(NGO, subject_PID), CRM.P2_has_type, getattr(AAT, aat_number)))
        new_graph.add((getattr(AAT, aat_number), CRM.P1i_identifies, Literal(aat_type)))

    return new_graph

def create_comment_triples(subject_PID, pred, obj):
    if pred == getattr(RRO, 'RP59.has_description'):
        new_graph.add((getattr(NGO, subject_PID), CRM.P3_has_note, Literal(obj)))

    return new_graph

def create_location_triples(subject_PID, subj, pred, obj):
    if pred == getattr(RRO, 'RP10.has_current_location') or pred == getattr(RRO, 'RP90.is_located_within'):
        location_PID = BNode()

        new_graph.add((getattr(NGO, subject_PID), CRM.P53_has_former_or_current_location, location_PID))
        new_graph.add((location_PID, CRM.P2_has_type, CRM.E53_Place))
        new_graph.add((location_PID, CRM.P1i_identifies, Literal(obj)))

    return new_graph

def create_actor_event_relationship_triples(subject_PID, pred, obj):
    if pred == getattr(RRO, 'RP43.was_carried_out_by'):
        actor_PID = create_PID_from_triple('object', obj)

        new_graph.add((getattr(NGO, subject_PID), CRM.P14_carried_out_by, getattr(NGO, actor_PID)))
        new_graph.add((getattr(NGO, actor_PID), CRM.P2_has_type, CRM.E39_Actor))
        new_graph.add((getattr(NGO, actor_PID), CRM.P1i_identifies, Literal(obj)))
    if pred == getattr(RRO, 'RP9.has_curator'):
        curator_PID = generate_placeholder_PID(obj)

        new_graph.add((getattr(NGO, subject_PID), CRM.P109_has_current_or_former_curator, getattr(NGO, curator_PID)))
        new_graph.add((getattr(NGO, curator_PID), CRM.P2_has_type, CRM.E39_Actor))
        new_graph.add((getattr(NGO, curator_PID), CRM.P1i_identifies, Literal(obj)))
    if pred == getattr(RRO, 'RP201.is_current_keeper_of'):
        collection_PID = create_PID_from_triple('object', obj)

        new_graph.add((getattr(NGO, subject_PID), CRM.P50i_is_current_keeper_of, getattr(NGO, collection_PID)))
        new_graph.add((getattr(NGO,collection_PID), CRM.P2_has_type, CRM.E78_Curated_Holding))
        new_graph.add((getattr(NGO,collection_PID), CRM.P1i_identifies, Literal(obj)))
    if pred == getattr(RRO, 'RP40.is_related_to'):
        object_PID = generate_placeholder_PID(obj)

        new_graph.add((getattr(NGO, subject_PID), CRM.E73_refers_to, getattr(NGO, object_PID)))

    return new_graph

def create_documentation_triples(subject_PID, pred, obj):
    if pred == getattr(RRO, 'RP245.has_website') or pred == getattr(RRO, 'RP257.has_external_link'):
        website_PID = BNode()

        new_graph.add((getattr(NGO, subject_PID), CRM.P70i_is_documented_in, website_PID))
        new_graph.add((website_PID, CRM.P2_has_type, CRM.E73_Information_Object))
        new_graph.add((website_PID, CRM.P1i_identifies, Literal(obj)))

    return new_graph

def create_institution_triples(old_graph, new_graph, subject_PID, institution_name):
    for subj, pred, obj in old_graph.triples((institution_name, None, None)):
        new_graph = create_type_triples(subject_PID, pred, obj)
        new_graph = create_comment_triples(subject_PID, pred, obj)
        new_graph = create_location_triples(subject_PID, subj, pred, obj)
        new_graph = create_actor_event_relationship_triples(subject_PID, pred, obj)
        new_graph = create_role_triples(subject_PID, pred, obj)
        new_graph = create_documentation_triples(subject_PID, pred, obj)
        new_graph = create_collection_triples(subject_PID, subj, pred, obj)
        new_graph = create_identifier_triples(subject_PID, pred, obj)

        if obj == getattr(RRO, 'RC10.Building'):
            object_PID = create_PID_from_triple('building', subj)
            new_graph.add((getattr(NGO, subject_PID), CRM.P156_occupies, getattr(NGO, object_PID)))

    return new_graph

def create_building_triples(old_graph, new_graph, subject_PID, building_name):
    for subj, pred, obj in old_graph.triples((building_name, None, None)):
        new_graph = create_location_triples(subject_PID, subj, pred, obj)
        create_type_triples(subject_PID, pred, obj)
        new_graph = create_identifier_triples(subject_PID, pred, obj)

    return new_graph

def create_room_triples(subject_PID, subj, pred, obj):
    if pred == getattr(RRO, 'RP90.is_located_within'):
        obj_PID = create_PID_from_triple('building', obj)
        new_graph.add((getattr(NGO, subject_PID), CRM.P1i_identifies, Literal(subj)))
        new_graph.add((getattr(NGO, subject_PID), CRM.P89_falls_within, getattr(NGO, obj_PID)))

    return new_graph

def create_reference_triples(subject_PID, subj, pred, obj):
    if pred == getattr(RRO, 'RP84.has_reference'):
        creation_event = BNode()
        reference_json = run_ruby_program(obj)
        title = reference_json[0]["title"][0]
        title_PID = BNode()
        date = reference_json[0]["date"][0]
        journal_title = reference_json[0]["container-title"][0]
        journal_title_PID = BNode()
        start_date, end_date = create_year_dates(date)
        time_span_PID = BNode()

        for i in range(0, len(reference_json[0]["author"])):
            author_node = BNode()
            author = reference_json[0]["author"][i]["given"] + " " + reference_json[0]["author"][i]["family"]
            new_graph.add((creation_event, CRM.P14_carried_out_by, author_node))
            new_graph.add((author_node, CRM.P2_has_type, CRM.E39_Actor))
            new_graph.add((author_node, CRM.P1i_identifies, Literal(author)))

        new_graph.add((getattr(NGO, subject_PID), CRM.P102_has_title, title_PID))
        new_graph.add((title_PID, CRM.P1i_identifies, Literal(title)))
        new_graph.add((title_PID, CRM.P106i_forms_part_of, journal_title_PID))
        new_graph.add((journal_title_PID, CRM.P1i_identifies, Literal(journal_title)))
        new_graph.add((getattr(NGO, subject_PID), CRM.P94i_was_created_by, creation_event))
        new_graph.add((creation_event, CRM.P2_has_type, CRM.E65_Creation))

        new_graph.add((getattr(NGO, subject_PID), CRM.P4_has_time_span, time_span_PID))
        new_graph.add((time_span_PID, CRM.P2_has_type, CRM.E52_Time_span))
        new_graph.add((time_span_PID, CRM.P2_has_type, getattr(AAT, '300379244')))
        new_graph.add((getattr(AAT, '300379244'), CRM.P1i_identifies, Literal('years@en')))
        wikidata_year = wikidata_query(date, 'year')
        new_graph.add((time_span_PID, CRM.P82a_begin_of_the_begin, Literal(start_date, datatype=XSD.dateTime)))
        new_graph.add((time_span_PID, CRM.P82b_end_of_the_end, Literal(end_date, datatype=XSD.dateTime)))
        if wikidata_year != None:
            new_graph.add((time_span_PID, OWL.sameAs, getattr(WD, wikidata_year)))
        
    return new_graph

def create_file_triples(subject_PID, pred, obj):
    if pred == getattr(RRO, 'RP95.has_file_name'):
        file_name = BNode()

        new_graph.add((getattr(NGO, subject_PID), CRM.P149_is_identified_by, file_name))
        new_graph.add((file_name, CRM.P2_has_type, CRM.E42_Identifier))
        new_graph.add((file_name, CRM.P2_has_type, getattr(WD, 'Q1144928')))
        new_graph.add((file_name, CRM.P1i_identifies, Literal(obj)))
        new_graph.add((getattr(WD, 'Q1144928'), CRM.P1i_identifies, Literal('filename@en')))
    elif pred == getattr(RRO, 'RP15.has_format'):
        file_format = BNode()

        new_graph.add((getattr(NGO, subject_PID), CRM.P2_has_type, file_format))
        if obj == getattr(RRI, 'RCL88.PDF'):
            new_graph.add((file_format, CRM.P2_has_type, getattr(AAT, '300266022')))
            new_graph.add((getattr(AAT, '300266022'), CRM.P1i_identifies, Literal('PDF@en')))

    elif pred == getattr(RRO, 'RP14.has_file_size'):
        file_size = BNode()

        new_graph.add((getattr(NGO,subject_PID), CRM.P43_has_dimension, file_size))
        new_graph.add((file_size, CRM.P2_has_type, CRM.E54_Dimension))
        new_graph.add((file_size, CRM.P90_has_value, Literal(obj, datatype=XSD.double)))
        new_graph.add((file_size, CRM.P91_has_unit, getattr(AAT,'300265869')))
        new_graph.add((getattr(AAT,'300265869'), CRM.P1i_identifies, Literal('bytes@en')))
        new_graph.add((file_size, CRM.P2_has_type, getattr(AAT,'300265863')))
        new_graph.add((getattr(AAT,'300265863'), CRM.P1i_identifies, Literal('size for computer files@en')))

    return new_graph
    
def map_object(old_graph, new_graph):
    for painting_id, _, _ in old_graph.triples((None, RDF.type, getattr(RRO,'RC12.Painting'))):
        for subj, pred, obj in old_graph.triples((painting_id, None, None)):
            subject_PID = generate_placeholder_PID(subj)
            new_graph = create_title_triples(subject_PID, subj, pred, obj)
            new_graph = create_medium_triples(subject_PID, subj, pred, obj)
            new_graph = create_collection_triples(subject_PID, subj, pred, obj)
            new_graph = create_dimension_triples(subject_PID, subj, pred, obj)
            new_graph = create_identifier_triples(subject_PID, pred, obj)
            new_graph = create_type_triples(subject_PID, pred, obj)
            new_graph = create_location_triples(subject_PID, subj, pred, obj)

    return new_graph

def map_event(old_graph, new_graph):
    event_properties_list = [
        'RP72.was_produced',
        'RP68.was_acquired',
        'RP42.was_born_in',
        'RP4.died_in'
    ]

    for event_property in event_properties_list:
        for entity, _, event_name in old_graph.triples((None, getattr(RRO, event_property), None)):
            for subj, pred, obj in old_graph.triples((entity, None, event_name)):
                subject_PID = generate_placeholder_PID(subj)
                event_PID = generate_placeholder_PID(obj)
                new_graph = create_event_triples(subject_PID, event_PID, subj, pred)
            for subj, pred, obj in old_graph.triples((event_name, None, None)):
                subject_PID = generate_placeholder_PID(subj)
                new_graph = create_time_span_triples(subject_PID, subj, pred, obj)
                new_graph = create_comment_triples(subject_PID, pred, obj)
                new_graph = create_actor_event_relationship_triples(subject_PID, pred, obj)

    return new_graph

def map_person(old_graph, new_graph):
    for person_name, _, _ in old_graph.triples((None, RDF.type, getattr(RRO, 'RC40.Person'))):
        for subj, pred, obj in old_graph.triples((person_name, None, None)):
            subject_PID = generate_placeholder_PID(subj)
            new_graph = create_type_triples(subject_PID, pred, obj)
            new_graph = create_name_triples(subject_PID, subj, pred, obj)
            new_graph = create_role_triples(subject_PID, pred, obj)
            new_graph = create_comment_triples(subject_PID, pred, obj)
            new_graph = create_location_triples(subject_PID, subj, pred, obj)

    return new_graph

def map_institution(old_graph, new_graph):
    for institution_name, _, _ in old_graph.triples((None, RDF.type, getattr(RRO, 'RC41.Institution'))):
        subject_PID = generate_placeholder_PID(institution_name)
        
        wikidata_name = wikidata_query(institution_name, 'institution')
        if wikidata_name is not None:
            new_graph.add((getattr(NGO, subject_PID), OWL.sameAs, Literal(wikidata_name)))
        
        new_graph = create_institution_triples(old_graph, new_graph, subject_PID, institution_name)
    
    for building_name, _, _ in old_graph.triples((None, RDF.type, getattr(RRO, 'RC10.Building'))):
        subject_PID = create_PID_from_triple('building', building_name)

        new_graph = create_building_triples(old_graph, new_graph, subject_PID, building_name)

    for room_name, _, _ in old_graph.triples((None, RDF.type, getattr(RRO, 'RC11.Room'))):
        subject_PID = generate_placeholder_PID(room_name)

        for subj, pred, obj in old_graph.triples((room_name, None, None)):
            new_graph = create_room_triples(subject_PID, subj, pred, obj)
    
    return new_graph

def map_document(old_graph, new_graph):
    for doc_name, _, _ in old_graph.triples((None, RDF.type, getattr(RRO, 'RC26.Digital_Document'))):
        subject_PID = generate_placeholder_PID(doc_name)

        for subj, pred, obj in old_graph.triples((doc_name, None, None)):
            new_graph = create_type_triples(subject_PID, pred, obj)
            new_graph = create_reference_triples(subject_PID, subj, pred, obj)
            new_graph = create_file_triples(subject_PID, pred, obj)
            new_graph = create_actor_event_relationship_triples(subject_PID, pred, obj)

    return new_graph

#def output_example_for_visualisation(new_graph):


#new_graph = map_property(g, RDF.type, RDFS.subClassOf)
#query_graph(new_graph,None,RDFS.subClassOf,None)

#new_dataframe = sparql_query_pythonic('NG1171',csv_format=False)
#mapped_dataframe = map_property(new_dataframe,'NG1171','PID')
#print(mapped_dataframe)
#triples_to_csv(new_dataframe)

#generate_placeholder_PID('Fake Painting 3')

#query_graph(g, None, getattr(RRO,'RP34.has_title'), None)
#map_property(g, new_graph, getattr(RRO,'RP34.has_title'), getattr(CRM,'P102_Has_Title'))
#query_graph(g, None, getattr(RRO, 'RP15.has_format'), None)
#triples_to_csv(new_graph,'crm_mapping_draft')

#map_event(g, new_graph)
#map_object(g, new_graph)
#map_institution(g, new_graph)
#map_person(g, new_graph)
map_document(g, new_graph)
for x,y,z in new_graph:
    print(x.n3(new_graph.namespace_manager), y.n3(new_graph.namespace_manager), z.n3(new_graph.namespace_manager))
triples_to_tsv(new_graph,'document')
#new_graph.serialize(destination='outputs/new_graph.xml', format='xml')
#print('Serialized to RDF')
