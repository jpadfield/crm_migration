import pandas as pd
import mysql.connector
from rdflib import Graph, Namespace, Literal, BNode
from rdflib.namespace import OWL, RDF, RDFS, NamespaceManager, XSD
from pdb import set_trace as st
import numpy as np
from common_functions import generate_placeholder_PID, triples_to_csv, triples_to_tsv, create_PID_from_triple, find_aat_value, run_ruby_program, wikidata_query, create_year_dates, connect_to_sql
from mapping_funcs import map_object, map_image, map_sample, map_event, map_extra_timespan_info, map_person, map_place
from os import path

def create_graph():

    RRO = Namespace("https://rdf.ng-london.org.uk/raphael/ontology/")
    RRI = Namespace("https://rdf.ng-london.org.uk/raphael/resource/")
    CRM = Namespace("http://www.cidoc-crm.org/cidoc-crm/")
    NGO = Namespace("https://data.ng-london.org.uk/")
    AAT = Namespace("http://vocab.getty.edu/page/aat/")
    TGN = Namespace("http://vocab.getty.edu/page/tgn/")
    WD = Namespace("http://www.wikidata.org/entity/")
    SCI = Namespace("http://www.cidoc-crm.org/crmsci/")
    DIG = Namespace("http://www.cidoc-crm.org/crmdig/")

    new_graph = Graph()
    new_graph.namespace_manager.bind('crm',CRM)
    new_graph.namespace_manager.bind('ngo',NGO)
    new_graph.namespace_manager.bind('aat',AAT)
    new_graph.namespace_manager.bind('tgn',TGN)
    new_graph.namespace_manager.bind('wd',WD)
    new_graph.namespace_manager.bind('rro',RRO)
    new_graph.namespace_manager.bind('rri',RRI)
    new_graph.namespace_manager.bind('sci',SCI)
    new_graph.namespace_manager.bind('dig', DIG)

    return new_graph

def pretty_print_triples():
    for x,y,z in new_graph:
        print(x.n3(new_graph.namespace_manager), y.n3(new_graph.namespace_manager), z.n3(new_graph.namespace_manager))

def sql_query(query):
    db = connect_to_sql()
    
    query_result_df = pd.read_sql(query, db)

    return query_result_df

def read_table(table):
    db = connect_to_sql()

    query = "SELECT * FROM " + table
    table_df = pd.read_sql(query, db)

    return table_df

def generate_table_dataframes():
    db = connect_to_sql()
    cursor = db.cursor()
    table_names_query = "SHOW TABLES;"
    cursor.execute(table_names_query)
    result = cursor.fetchall()

    tables_list = [x[0] for x in result]
    tables = [read_table(table) for table in tables_list]

    tables_dict = {table_name:table for table_name, table in zip(tables_list, tables)}
    
    return tables_dict

def map_db_to_triples(full_rebuild=False):
    # Pulling in all tables from the sql database and extracting the ones we need into their own objects
    tables_dict = generate_table_dataframes()
    
    object_medium_support = tables_dict["object_medium_support"]
    object_part_title_table = tables_dict["object_part_title_table"]
    obj_reference_timespan = tables_dict["obj_reference_timespan_f"]
    person_parent_table = tables_dict["person_parent_table"]
    object_event_influence = tables_dict["object_event_influence"]
    person_influence = tables_dict["person_influence"]
    parent_influence = tables_dict["parent_influence"]
    person_role_institution = tables_dict["person_role_institution"]
    place_institution_parent = tables_dict["place_institution_parent"]
    place_timespan = tables_dict["place_timespan"]
    full_timespan = tables_dict["full_timespan"]
    sample_colour = tables_dict["sample_colour"]
    sample_component_view = tables_dict["sample_component_view"]
    sample_component_colours = tables_dict["sample_component_colours"]
    sample_component_parents = tables_dict["sample_component_parents"]
    sample_event = tables_dict["sample_event"]
    preparation_colours = tables_dict["preparation_colours"]
    sample_timespan_event = tables_dict["sample_timespan_event"]
    event_protocol = tables_dict["event_protocol"]
    measurement_materials = tables_dict["measurement_materials"]
    image_path_server_etc = tables_dict["image_path_server_etc"]
    model_path_server_etc = tables_dict["model_path_server_etc"]
    institution_classification = tables_dict["institution_classification"]
    sample_reference = tables_dict["sample_reference"]
    
    # Checking whether specific sections have already been mapped and pulling them in or building them
    if path.exists('outputs/grounds_object.xml') == False or full_rebuild == True:
        object_graph = create_graph()
        new_object_graph = map_object(new_graph=object_graph, object_medium_support=object_medium_support, object_part_title_table=object_part_title_table, obj_reference_timespan=obj_reference_timespan, preparation_colours=preparation_colours)
        new_object_graph.serialize(destination='outputs/grounds_object.xml', format='xml')
        new_object_graph.serialize(destination='outputs/grounds_object.ttl', format='ttl')
        new_object_graph.serialize(destination='outputs/grounds_object.trig', format='trig')
    else:
        new_object_graph = Graph()
        new_object_graph = new_object_graph.parse('outputs/grounds_object.xml')
    print('objects mapped!')

    if path.exists('outputs/grounds_event.xml') == False or full_rebuild == True:
        event_graph = create_graph()
        new_event_graph = map_event(new_graph=event_graph, person_parent_table=person_parent_table, object_event_influence=object_event_influence, event_protocol=event_protocol, sample_event=sample_event)
        new_event_graph.serialize(destination='outputs/grounds_event.xml', format='xml')
        new_event_graph.serialize(destination='outputs/grounds_event.ttl', format='ttl')
        new_event_graph.serialize(destination='outputs/grounds_event.trig', format='trig')
    else:
        new_event_graph = Graph()
        new_event_graph = new_event_graph.parse('outputs/grounds_event.xml')
    print('events mapped!')

    if path.exists('outputs/grounds_person.xml') == False or full_rebuild == True:
        person_graph = create_graph()
        new_person_graph = map_person(new_graph=person_graph, person_role_institution=person_role_institution, parent_influence=parent_influence, person_influence=person_influence)
        new_person_graph.serialize(destination='outputs/grounds_person.xml', format='xml')
        new_person_graph.serialize(destination='outputs/grounds_person.ttl', format='ttl')
        new_person_graph.serialize(destination='outputs/grounds_person.trig', format='trig')
    else:
        new_person_graph = Graph()
        new_person_graph = new_person_graph.parse('outputs/grounds_person.xml')
    print('people mapped!')

    if path.exists('outputs/grounds_place.xml') == False or full_rebuild == True:
        place_graph = create_graph()
        new_place_graph = map_place(new_graph=place_graph, place_institution_parent=place_institution_parent, place_timespan=place_timespan, institution_classification=institution_classification)
        new_place_graph.serialize(destination='outputs/grounds_place.xml', format='xml')
        new_place_graph.serialize(destination='outputs/grounds_place.ttl', format='ttl')
        new_place_graph.serialize(destination='outputs/grounds_place.trig', format='trig')
    else:
        new_place_graph = Graph()
        new_place_graph = new_place_graph.parse('outputs/grounds_place.xml')
    print('places mapped!')
    
    if path.exists('outputs/grounds_timespan.xml') == False or full_rebuild == True:
        extra_timespan_graph = create_graph()
        new_extra_timespan_graph = map_extra_timespan_info(new_graph=extra_timespan_graph, full_timespan=full_timespan)
        new_extra_timespan_graph.serialize(destination='outputs/grounds_timespan.xml', format='xml')
        new_extra_timespan_graph.serialize(destination='outputs/grounds_timespan.ttl', format='ttl')
        new_extra_timespan_graph.serialize(destination='outputs/grounds_timespan.trig', format='trig')
    else:
        new_extra_timespan_graph = Graph()
        new_extra_timespan_graph = new_extra_timespan_graph.parse('outputs/grounds_timespan.xml')
    print('timespans mapped!')

    if path.exists('outputs/grounds_sample.xml') == False or full_rebuild == True:
        sample_graph = create_graph()
        new_sample_graph = map_sample(new_graph=sample_graph, sample_colour=sample_colour, sample_timespan_event=sample_timespan_event, sample_component_view=sample_component_view, sample_component_colours=sample_component_colours, sample_component_parents=sample_component_parents, sample_reference=sample_reference)
        new_sample_graph.serialize(destination='outputs/grounds_sample.xml', format='xml')
        new_sample_graph.serialize(destination='outputs/grounds_sample.ttl', format='ttl')
        new_sample_graph.serialize(destination='outputs/grounds_sample.trig', format='trig')
    else:
        new_sample_graph = Graph()
        new_sample_graph = new_sample_graph.parse('outputs/grounds_sample.xml')
    print('samples mapped!')

    if path.exists('outputs/grounds_image.xml') == False or full_rebuild == True:
        image_graph = create_graph()
        new_image_graph = map_image(new_graph=image_graph, image_path_server_etc=image_path_server_etc, model_path_server_etc=model_path_server_etc)
        new_image_graph.serialize(destination='outputs/grounds_image.xml', format='xml')
        new_image_graph.serialize(destination='outputs/grounds_image.ttl', format='ttl')
        new_image_graph.serialize(destination='outputs/grounds_image.trig', format='trig')
    else:
        new_image_graph = Graph()
        new_image_graph = new_image_graph.parse('outputs/grounds_image.xml')
    print('images mapped!')
    
    full_graph = Graph()
    full_graph.parse(new_object_graph)
    full_graph.parse(new_event_graph)
    full_graph.parse(new_person_graph)
    full_graph.parse(new_place_graph)
    full_graph.parse(new_extra_timespan_graph)
    full_graph.parse(new_sample_graph)
    full_graph.parse(new_image_graph)

    return full_graph

def main():
    # Initializing the mapping and outputting the full graph in three formats - pass "full_rebuild=True" to map_db_to_triples() for a full rebuild
    full_graph = map_db_to_triples()
    full_graph.serialize(destination='outputs/grounds_full.xml', format='xml')
    full_graph.serialize(destination='outputs/grounds_full.ttl', format='ttl')
    full_graph.serialize(destination='outputs/grounds_full.trig', format='trig')

if __name__ == '__main__':
    main()