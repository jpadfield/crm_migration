from rdflib import Graph, Namespace, Literal, BNode
from rdflib.namespace import RDF, RDFS, NamespaceManager, XSD
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
import re
from mapping_functions import map_object, map_event, map_image, map_institution, map_person, map_document, map_sample, map_leftover_categories
from common_functions import connect_to_sql 

def main():
    RRO = Namespace("https://rdf.ng-london.org.uk/raphael/ontology/")
    RRI = Namespace("https://rdf.ng-london.org.uk/raphael/resource/")
    CRM = Namespace("http://www.cidoc-crm.org/cidoc-crm/")
    NGO = Namespace("https://data.ng-london.org.uk/")
    AAT = Namespace("http://vocab.getty.edu/page/aat/")
    TGN = Namespace("http://vocab.getty.edu/page/tgn/")
    WD = Namespace("http://www.wikidata.org/entity/")
    DIG = Namespace("http://www.cidoc-crm.org/crmdig/")
    SCI = Namespace("http://www.cidoc-crm.org/crmsci/")
    OWL = Namespace("http://www.w3.org/2002/07/owl#")

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
    new_graph.namespace_manager.bind('dig', DIG)
    new_graph.namespace_manager.bind('sci', SCI)
    new_graph.namespace_manager.bind('owl', OWL)   

    db = connect_to_sql() 

    #map_object(g, new_graph)
    map_event(g, new_graph)
    #map_image(g, new_graph)
    #map_institution(g, new_graph)
    #map_person(g, new_graph)
    #map_document(g, new_graph)
    #map_sample(g, new_graph)
    #map_leftover_categories(g, new_graph)

    new_graph.serialize(destination='outputs/raphael_final.xml', format='xml')
    new_graph.serialize(destination='outputs/raphael_final.ttl', format='ttl')
    new_graph.serialize(destination='outputs/raphael_final.trig', format='trig')
    print('Finished running, all looking good')

if __name__ == '__main__':
    main()