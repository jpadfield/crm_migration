import pandas as pd
import mysql.connector
from rdflib import Graph, Namespace, Literal, BNode
from rdflib.namespace import OWL, RDF, RDFS, NamespaceManager, XSD
from pdb import set_trace as st
import numpy as np
from common_functions import generate_placeholder_PID, triples_to_csv, triples_to_tsv, create_PID_from_triple, find_aat_value, run_ruby_program, wikidata_query, create_year_dates, check_aat_values

RRO = Namespace("https://rdf.ng-london.org.uk/raphael/ontology/")
RRI = Namespace("https://rdf.ng-london.org.uk/raphael/resource/")
CRM = Namespace("http://www.cidoc-crm.org/cidoc-crm/")
NGO = Namespace("https://data.ng-london.org.uk/")
AAT = Namespace("http://vocab.getty.edu/page/aat/")
TGN = Namespace("http://vocab.getty.edu/page/tgn/")
WD = Namespace("http://www.wikidata.org/entity/")
SCI = Namespace("http://www.cidoc-crm.org/crmsci/")
DIG = Namespace("http://www.cidoc-crm.org/crmdig/")

def parse_reference_json(new_graph, reference_json, subject_PID):
    creation_event = BNode()
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
        new_graph.add((author_node, RDF.type, CRM.E39_Actor))
        new_graph.add((author_node, CRM.P2_has_type, CRM.E39_Actor))
        new_graph.add((author_node, RDFS.label, Literal(author, lang="en")))

    new_graph.add((getattr(NGO, subject_PID), CRM.P102_has_title, title_PID))
    new_graph.add((title_PID, RDFS.label, Literal(title, lang="en")))
    new_graph.add((title_PID, CRM.P106i_forms_part_of, journal_title_PID))
    new_graph.add((journal_title_PID, RDFS.label, Literal(journal_title, lang="en")))
    new_graph.add((getattr(NGO, subject_PID), CRM.P94i_was_created_by, creation_event))
    new_graph.add((creation_event, RDF.type, CRM.E65_Creation))
    new_graph.add((creation_event, CRM.P2_has_type, CRM.E65_Creation))

    new_graph.add((creation_event, CRM.P4_has_time_span, time_span_PID))
    new_graph.add((time_span_PID, RDF.type, CRM.E52_Time_span))
    new_graph.add((time_span_PID, CRM.P2_has_type, CRM.E52_Time_span))
    new_graph.add((time_span_PID, CRM.P2_has_type, getattr(AAT, '300379244')))
    new_graph.add((getattr(AAT, '300379244'), RDFS.label, Literal('years', lang="en")))
    wikidata_year = wikidata_query(date, 'year')
    new_graph.add((time_span_PID, CRM.P82a_begin_of_the_begin, Literal(start_date, datatype=XSD.dateTime)))
    new_graph.add((time_span_PID, CRM.P82b_end_of_the_end, Literal(end_date, datatype=XSD.dateTime)))
    if wikidata_year != None and wikidata_year != 'No WD value':
        new_graph.add((time_span_PID, OWL.sameAs, getattr(WD, wikidata_year)))

    return new_graph

def create_object_triples(new_graph, **kwargs):

    new_graph.add((getattr(NGO, kwargs["object_PID"]), RDF.type, CRM.E22_Human_Made_Object))
    new_graph.add((getattr(NGO, kwargs["object_PID"]), CRM.P1_is_identified_by, Literal(kwargs["obj"])))
    new_graph.add((getattr(NGO, kwargs["object_PID"]), CRM.P3_has_note, Literal(kwargs["comment"])))
    new_graph.add((getattr(NGO, kwargs["object_PID"]), CRM.P34_was_assessed_by, getattr(NGO, kwargs["assessment_event_PID"])))
    new_graph.add((getattr(NGO, kwargs["assessment_event_PID"]), CRM.P35_has_identified, getattr(NGO, kwargs["condition_PID"])))
    new_graph.add((getattr(NGO, kwargs["condition_PID"]), RDF.type, CRM.E3_Condition_State))

    return new_graph

def create_title_triples(new_graph, **kwargs):

    new_graph.add((getattr(NGO, kwargs["object_PID"]), CRM.P102_has_title, kwargs["title_PID"]))
    new_graph.add((kwargs["title_PID"], CRM.P1i_identifies, Literal(kwargs["long_title"])))
    new_graph.add((kwargs["title_PID"], RDF.type, CRM.E35_Title))
    new_graph.add((kwargs["title_PID"], RDF.type, getattr(AAT,'300417209')))
    new_graph.add((getattr(AAT,'300417209'), RDFS.label, Literal('full title@en')))

    new_graph.add((getattr(NGO, kwargs["object_PID"]), CRM.P102_has_title, kwargs["short_title_PID"]))
    new_graph.add((kwargs["short_title_PID"], CRM.P1i_identifies, Literal(kwargs["short_title"])))
    new_graph.add((kwargs["short_title_PID"], RDF.type, CRM.E35_Title))
    new_graph.add((kwargs["short_title_PID"], RDF.type, getattr(AAT,'300417208')))
    new_graph.add((getattr(AAT,'300417208'), RDFS.label, Literal('brief title@en')))

    new_graph.add((getattr(NGO, kwargs["title_PID"]), CRM.P3_has_note, Literal(kwargs["title_comment"])))

    return new_graph

def create_dimension_triples(new_graph, **kwargs):

    new_graph.add((getattr(NGO, kwargs["object_PID"]), CRM.P43_has_dimension, kwargs["dimension_PID"]))
    new_graph.add((kwargs["dimension_PID"], RDF.type, CRM.E54_Dimension))
    new_graph.add((kwargs["dimension_PID"], CRM.P90_has_value, Literal(kwargs["obj"], datatype=XSD.double)))
    new_graph.add((kwargs["dimension_PID"], CRM.P91_has_unit, getattr(AAT,kwargs["aat_unit_value"])))
    new_graph.add((getattr(AAT,kwargs["aat_unit_value"]), RDFS.label, Literal(kwargs["aat_unit_title"])))
    new_graph.add((kwargs["dimension_PID"], RDF.type, getattr(AAT,kwargs["aat_dimension_value"])))
    new_graph.add((getattr(AAT,kwargs["aat_dimension_value"]), RDFS.label, Literal(kwargs["aat_dimension_title"])))

    return new_graph

def create_event_triples(new_graph, **kwargs):

    new_graph.add((getattr(NGO, kwargs["object_PID"]), kwargs["event_property"], getattr(NGO, kwargs["event_PID"])))
    new_graph.add((getattr(NGO, kwargs["event_PID"]), RDF.type, kwargs["event_type"]))
    new_graph.add((getattr(NGO, kwargs["event_PID"]), RDF.type, getattr(AAT, kwargs["aat_event_id"])))
    new_graph.add((getattr(AAT, kwargs["aat_event_id"]), RDFS.label, Literal(["aat_event_type"], lang="en")))
    
    if kwargs["related_painting_history_event"] is not None:
        new_graph.add((getattr(NGO, kwargs["event_PID"]), CRM.P10_falls_within, getattr(NGO, kwargs["related_painting_history_event_PID"])))
        new_graph.add((getattr(NGO, kwargs["related_painting_history_event_PID"]), RDF.type, CRM.E5_Event))
        new_graph.add((getattr(NGO, kwargs["related_painting_history_event_PID"]), RDF.type, getattr(AAT, '300055863')))
        new_graph.add((getattr(AAT, '300055863'), RDFS.label, Literal('provenance (history of ownership)@en')))
        new_graph.add((getattr(NGO, kwargs["related_painting_history_event_PID"]), RDFS.label, Literal(kwargs["related_painting_history_event"])))

    if kwargs["parent_PID"] is not None:
        new_graph.add((getattr(NGO, kwargs["event_PID"]), CRM.P96_by_mother, getattr(NGO, kwargs["parent_PID"])))
        new_graph.add((getattr(NGO, kwargs["object_PID"]), CRM.P152_has_parent, getattr(NGO, kwargs["parent_PID"])))

    return new_graph

def create_medium_triples(new_graph, **kwargs):

    new_graph.add((getattr(NGO,kwargs["object_PID"]), CRM.P45_consists_of, getattr(NGO, kwargs["medium_PID"])))
    new_graph.add((getattr(NGO, kwargs["medium_PID"]), RDF.type, CRM.E57_Material))
    new_graph.add((getattr(NGO, kwargs["medium_PID"]), CRM.P3_has_note, Literal(kwargs["comment"])))
    new_graph.add((getattr(NGO, kwargs["medium_PID"]), RDFS.label, Literal(kwargs["medium_name"])))

    if kwargs["aat_number"] is not None:
        for aat_number, aat_type in zip(kwargs["aat_number"], kwargs["aat_type"]):
            
            new_graph.add((getattr(NGO, kwargs["medium_PID"]), RDF.type, getattr(AAT,aat_number)))
            new_graph.add(((getattr(AAT,aat_number)), RDFS.label, Literal(aat_type)))
    
    if kwargs["material_type"] == 'medium':

        new_graph.add(((getattr(AAT,'300163343')), RDFS.label, Literal('media (artists\' material)@en')))
        new_graph.add((getattr(NGO, kwargs["medium_PID"]), RDF.type, getattr(AAT,'300163343')))
        
    elif kwargs["material_type"] == 'support':

        new_graph.add((getattr(NGO, kwargs["medium_PID"]), RDF.type, getattr(AAT,'300014844')))
        new_graph.add(((getattr(AAT,'300014844')), RDFS.label, Literal('supports (artists\' materials)@en')))

    return new_graph

def create_object_part_triples(new_graph, **kwargs):
    obj_part_type = BNode()

    new_graph.add((getattr(NGO, kwargs["object_PID"]), CRM.P46_is_composed_of, getattr(NGO, kwargs["part_PID"])))
    new_graph.add((getattr(NGO, kwargs["part_PID"]), RDF.type, CRM.E26_Physical_Feature))
    new_graph.add((getattr(NGO, kwargs["part_PID"]), RDFS.label, Literal(kwargs["part_label"])))
    new_graph.add((getattr(NGO, kwargs["part_PID"]), CRM.P3_has_note, Literal(kwargs["comment"])))
    new_graph.add((getattr(NGO, kwargs["part_PID"]), CRM.P2_has_type, obj_part_type))
    new_graph.add((obj_part_type, RDFS.label, Literal(kwargs["object_part_type"])))
    new_graph.add((obj_part_type, RDF.type, CRM.E55_Type))

    return new_graph

def create_timespan_triples(new_graph, **kwargs):
    timespan_PID = kwargs["timespan_PID"]
    event_name = str(kwargs["event_name"])
    timespan_name = str(kwargs["timespan_name"])

    new_graph.add((getattr(NGO, kwargs["event_PID"]), CRM.P4_has_time_span, getattr(NGO, kwargs["timespan_PID"])))
    new_graph.add((getattr(NGO, kwargs["timespan_PID"]), RDF.type, CRM.E52_Time_span))
    new_graph.add((getattr(NGO, kwargs["timespan_PID"]), RDF.type, getattr(AAT, '300379244')))
    new_graph.add((getattr(AAT, '300379244'), RDFS.label, Literal('years@en')))
    new_graph.add((getattr(NGO, kwargs["timespan_PID"]), RDFS.label, Literal("Timespan of " + event_name, lang="en")))
    new_graph.add((getattr(NGO, kwargs["timespan_PID"]), CRM.P82a_begin_of_the_begin, Literal(kwargs["timespan_start"], datatype=XSD.dateTime)))
    new_graph.add((getattr(NGO, kwargs["timespan_PID"]), CRM.P82b_end_of_the_end, Literal(kwargs["timespan_end"], datatype=XSD.dateTime)))

    if kwargs["timespan_descriptor"] != '' and kwargs["timespan_descriptor"] is not None:
        new_graph.add((getattr(NGO, kwargs["timespan_PID"]), CRM.P90_has_value, Literal(kwargs["timespan_descriptor"] + timespan_name, lang="en")))
    else:
        new_graph.add((getattr(NGO, kwargs["timespan_PID"]), CRM.P90_has_value, Literal(timespan_name, lang="en")))

    if kwargs["timespan_confidence"] is not None and kwargs["timespan_confidence"] != '':

        new_graph.add((getattr(NGO, kwargs["timespan_descriptor"]), RDFS.label, getattr(NGO, kwargs["timespan_confidence"])))

    if kwargs["timespan_comment"] != '':

        new_graph.add((getattr(NGO, kwargs["timespan_PID"]), CRM.P3_has_note, Literal(kwargs["timespan_comment"], lang="en")))

    return new_graph

def create_person_triples(new_graph, **kwargs):
    name_PID = BNode()

    new_graph.add((getattr(NGO, kwargs["person_PID"]), CRM.P1_is_identified_by, name_PID))
    new_graph.add((getattr(NGO, kwargs["person_PID"]), CRM.P48_has_preferred_identifier, name_PID))
    new_graph.add((name_PID, RDF.type, CRM.E41_Appellation))
    new_graph.add((name_PID, RDFS.label, Literal(kwargs["person_name"])))

    if kwargs["comment"] != "":
        new_graph.add((name_PID, CRM.P3_has_note, Literal(kwargs["comment"])))

    if kwargs["person_other_name"] != "":
        other_name_PID = BNode()

        new_graph.add((getattr(NGO, kwargs["person_PID"]), CRM.P1_is_identified_by, other_name_PID))
        new_graph.add((other_name_PID, RDF.type, CRM.E41_Appellation))
        new_graph.add((other_name_PID, RDFS.label, Literal(kwargs["person_other_name"])))

    if kwargs["person_contact"] != "":
        contact_PID = BNode()

        new_graph.add((getattr(NGO, kwargs["person_PID"]), CRM.P76_has_contact_point, contact_PID))
        new_graph.add((contact_PID, RDF.type, CRM.E41_Appellation))
        new_graph.add((contact_PID, RDFS.label, Literal(kwargs["person_contact"])))

    return new_graph

def create_role_triples(new_graph, **kwargs):

    role_PID = BNode()

    new_graph.add((getattr(NGO, kwargs["person_PID"]), CRM.P1_is_identified_by, role_PID))
    new_graph.add((role_PID, RDF.type, CRM.E41_Appellation))
    new_graph.add((role_PID, RDFS.label, Literal(kwargs["role_name"])))
    new_graph.add((getattr(NGO, kwargs["institution_PID"]), CRM.P107_has_current_or_former_member, getattr(NGO, kwargs["person_PID"])))

    if kwargs["person_title"] != "":
        
        try:
            aat_number, aat_type = find_aat_value(kwargs["person_title"], 'roles')

            new_graph.add((role_PID, RDF.type, getattr(AAT, aat_number)))
            new_graph.add((getattr(AAT, aat_number), RDFS.label, Literal(aat_type)))
        except:
            return

    if kwargs["role_comment"] != "":

        new_graph.add((role_PID, CRM.P3_has_note, Literal(kwargs["role_comment"])))

    return new_graph

def create_influence_triples(new_graph, **kwargs):

    new_graph.add((getattr(NGO, kwargs["influenced_event_PID"]), CRM.P15_was_influenced_by, getattr(NGO, kwargs["influence_PID"])))
    new_graph.add((getattr(NGO, kwargs["influenced_event_PID"]), RDFS.label, Literal(kwargs["influenced_event_name"], lang="en")))
    new_graph.add((getattr(NGO, kwargs["influenced_event_PID"]), RDF.type, CRM.P65_Creation))
    new_graph.add((getattr(NGO, kwargs["influenced_event_PID"]), CRM.P14_carried_out_by, getattr(NGO, kwargs["person_PID"])))
    new_graph.add((getattr(NGO, kwargs["influence_PID"]), RDF.type, CRM.E74_Group))
    new_graph.add((getattr(NGO, kwargs["influence_PID"]), RDFS.label, Literal(kwargs["influence_name"], lang="en")))

    if kwargs["aat_link"] != "":

        new_graph.add((getattr(NGO, kwargs["influence_PID"]), CRM.P2_has_type, getattr(AAT, kwargs["aat_link"])))

    if kwargs["comment"] != "":
        new_graph.add((getattr(NGO, kwargs["influence_PID"]), CRM.P3_has_note, Literal(kwargs["comment"], lang="en")))

    return new_graph

def create_institution_triples(new_graph, **kwargs):

    new_graph.add((getattr(NGO, kwargs["institution_PID"]), RDF.type, CRM.E74_Group))
    new_graph.add((getattr(NGO, kwargs["institution_PID"]), CRM.P2_has_type, CRM.E74_Group))
    new_graph.add((getattr(NGO, kwargs["institution_PID"]), RDFS.label, Literal(kwargs["institution_name"], lang="en")))
    new_graph.add((getattr(NGO, kwargs["institution_PID"]), CRM.P48_has_preferred_identifier, Literal(kwargs["institution_name"], lang="en")))
    new_graph.add((getattr(NGO, kwargs["institution_PID"]), CRM.P1_is_identified_by, Literal(kwargs["institution_acronym"], lang="en")))

    if kwargs["webpage"] != "":

        website_PID = BNode()

        new_graph.add((getattr(NGO, kwargs["institution_PID"]), CRM.P70i_is_documented_in, website_PID))
        new_graph.add((website_PID, RDF.type, CRM.E73_Information_Object))
        new_graph.add((website_PID, CRM.P2_has_type, CRM.E73_Information_Object))
        new_graph.add((website_PID, RDFS.label, Literal(kwargs["webpage"], lang="en")))
        new_graph.add((getattr(NGO, website_PID), CRM.P2_has_type, getattr(AAT, '300265431')))
        new_graph.add((getattr(AAT, '300265431'), RDFS.label, Literal('Web sites', lang="en")))

    if kwargs["institution_type"] != "":
        # THIS IS INCOMPLETE BECAUSE THERE ARE NO EXAMPLES!
        try:
            aat_ref, aat_literal = find_aat_value(kwargs["institution_type"], 'institution_type')

            new_graph.add((getattr(NGO, kwargs["institution_PID"]), CRM.P2_has_type, getattr(AAT, aat_ref)))
            new_graph.add((getattr(AAT, aat_ref), RDFS.label, Literal(aat_literal, lang="en")))
        except:
            print('no aat example')

    if kwargs["institution_comment"] != "":

        new_graph.add((getattr(NGO, kwargs["institution_PID"]), CRM.P3_has_note, Literal(kwargs["institution_comment"], lang="en")))

    return new_graph

def create_place_triples(new_graph, **kwargs):

    new_graph.add((getattr(NGO, kwargs["institution_PID"]), CRM.P156_occupies, getattr(NGO, kwargs["building_PID"])))
    new_graph.add((getattr(NGO, kwargs["building_PID"]), CRM.P53_has_former_or_current_location, getattr(NGO, kwargs["location_PID"])))
    new_graph.add((getattr(NGO, kwargs["location_PID"]), RDF.type, CRM.E53_Place))
    new_graph.add((getattr(NGO, kwargs["location_PID"]), CRM.P2_has_type, CRM.E53_Place))
    new_graph.add((getattr(NGO, kwargs["location_PID"]), RDFS.label, Literal(kwargs["place_name"], lang="en")))

    if kwargs["place_type"] != "":
        if kwargs["place_type"] == 'institute' or kwargs["place_type"] == 'museum':
            aat_ref = '300312292'
            aat_value = 'institutions (buildings)'
        elif kwargs["place_type"] == 'city':
            aat_ref = '300008389'
            aat_value = 'cities'
        elif kwargs["place_type"] == 'country':
            aat_ref = '300387506'
            aat_value = 'countries (sovereign states)'
        elif kwargs["place_type"] == 'state':
            aat_ref = '300000776'
            aat_value = 'states (political divisions)'
        elif kwargs["place_type"] == 'road':
            aat_ref = '300008217'
            aat_value = 'roads'
        elif kwargs["place_type"] == 'village':
            aat_ref = '300008372'
            aat_value = 'villages'
        elif kwargs["place_type"] == 'water':
            aat_ref = '300266059'
            aat_value = 'bodies of water (natural)'

        new_graph.add((getattr(NGO, kwargs["location_PID"]), CRM.P2_has_type, getattr(AAT, aat_ref)))
        new_graph.add((getattr(AAT, aat_ref), RDFS.label, Literal(aat_value, lang="en")))

    if kwargs["latitude"] != "":
        latitude = str(kwargs["latitude"])
        longitude = str(kwargs["longitude"])
        coordinates = latitude + 'N ' + longitude + 'E'

        new_graph.add((getattr(NGO, kwargs["location_PID"]), CRM.P168_place_is_defined_by, Literal(coordinates)))

    if kwargs["place_comment"] != "":

        new_graph.add((getattr(NGO, kwargs["location_PID"]), CRM.P3_has_note, Literal(kwargs["place_comment"], lang="en")))

    return new_graph

def create_extra_timespan_triples(new_graph, **kwargs):

    generic_timespan = BNode()

    if kwargs["timespan_extra_relation"] == 'within':
        new_graph.add((getattr(NGO, kwargs["timespan_PID"]), CRM.P175i_starts_after_or_with_the_start_of, generic_timespan))
        new_graph.add((getattr(NGO, kwargs["timespan_PID"]), CRM.P184_ends_before_or_with_the_end_of, generic_timespan))
    elif kwargs["timespan_extra_relation"] == 'contains':
        new_graph.add((getattr(NGO, kwargs["timespan_PID"]), CRM.P176_starts_before_the_start_of, generic_timespan))
        new_graph.add((getattr(NGO, kwargs["timespan_PID"]), CRM.P185i_ends_after_the_end_of, generic_timespan))
    elif kwargs["timespan_extra_relation"] == 'overlaps':
        new_graph.add((getattr(NGO, kwargs["timespan_PID"]), CRM.P132_spatiotemporally_overlaps_with, generic_timespan))
    
    if kwargs["timespan_extra_group"] == 'decade':
        aat_ref = '300379246'
        aat_title = 'decades'
    elif kwargs["timespan_extra_group"] == 'century':
        aat_ref = '300379247'
        aat_title = 'centuries'
    elif kwargs["timespan_extra_group"] is None: 
        aat_ref = 'undefined'
    
    if aat_ref != 'undefined':
        new_graph.add((generic_timespan, CRM.P2_has_type, getattr(AAT, aat_ref)))
        new_graph.add((getattr(AAT, aat_ref), RDFS.label, Literal(aat_title, lang="en")))
    new_graph.add((generic_timespan, RDF.type, CRM.E52_Time_Span))
    new_graph.add((generic_timespan, CRM.P2_has_type, CRM.E52_Time_Span))
    new_graph.add((generic_timespan, RDFS.label, Literal(kwargs["timespan_extra_name"], lang="en")))

    if kwargs["timespan_extra_comment"] != '':
        new_graph.add((generic_timespan, CRM.P3_has_note, Literal(kwargs["timespan_extra_comment"], lang="en")))

    return new_graph

def create_reference_triples(new_graph, **kwargs):

    reference_PID = kwargs["reference_PID"]
    if kwargs["thing_type"] == 'object':
        painting_PID = kwargs["object_PID"]
    elif kwargs["thing_type"] == 'sample':
        painting_PID = kwargs["sample_PID"]

    new_graph.add((getattr(NGO, reference_PID), CRM.P67_refers_to, getattr(NGO, painting_PID)))
    new_graph.add((getattr(NGO, reference_PID), RDF.type, CRM.E31_Document))
    new_graph.add((getattr(NGO, reference_PID), CRM.P2_has_type, CRM.E31_Document))
    new_graph.add((getattr(NGO, reference_PID), RDFS.label, Literal(kwargs["reference_title"])))

    if kwargs["reference_type"] == 'reference':
        aat_ref = '300311954'
        aat_type = 'references'
    elif kwargs["reference_type"] == 'report':
        aat_ref = '300027267'
        aat_type = 'reports'
    
    new_graph.add((getattr(NGO, reference_PID), CRM.P2_has_type, getattr(AAT, aat_ref)))
    new_graph.add((getattr(AAT, aat_ref), RDFS.label, Literal(aat_type, lang="en")))

    if kwargs["reference_comment"] is not None and kwargs["reference_comment"] != '':
        new_graph.add((getattr(NGO, reference_PID), CRM.P3_has_note, Literal(kwargs["reference_comment"], lang="en")))

    if kwargs["reference_link"] is not None and kwargs["reference_link"] != '':
        website_PID = BNode()
        
        new_graph.add((getattr(NGO, reference_PID), CRM.P70i_is_documented_in, website_PID))
        new_graph.add((website_PID, RDF.type, CRM.E73_Information_Object))
        new_graph.add((website_PID, CRM.P2_has_type, CRM.E73_Information_Object))
        new_graph.add((website_PID, RDFS.label, Literal(kwargs["reference_link"], lang="en")))
        new_graph.add((website_PID, CRM.P2_has_type, getattr(AAT, '300265431')))
        new_graph.add((getattr(AAT, '300265431'), RDFS.label, Literal('Web sites', lang="en")))

    return new_graph

def create_sample_triples(new_graph, **kwargs):
    sampling_event = kwargs["event_name"]
    sampling_event_PID = generate_placeholder_PID(sampling_event)
    subject_PID = kwargs["sample_PID"]
    sampled_section = kwargs["object_part_PID"]
    sample_site_label = kwargs["object_part_name"]
    sampled_work_PID = generate_placeholder_PID(kwargs["object_inventory_number"])
    national_gallery_staff_member_PID = generate_placeholder_PID('National Gallery staff member')
    doc_BN = BNode()
    doc2_BN = BNode()

    if kwargs["event_type"] == 'sampling':
        new_graph.add((getattr(NGO, sampling_event_PID), RDF.type, SCI.S2_Sample_Taking))
        new_graph.add((getattr(NGO, sampling_event_PID), CRM.P2_has_type, SCI.S2_Sample_Taking))
        new_graph.add((getattr(NGO, sampling_event_PID), SCI.O5_removed, getattr(NGO, subject_PID)))
        new_graph.add((getattr(NGO, sampling_event_PID), RDFS.label, Literal(sampling_event, lang="en")))
        new_graph.add((getattr(NGO, subject_PID), CRM.P2_has_type, getattr(AAT, '300028875')))
        new_graph.add((getattr(AAT, '300028875'), RDFS.label, Literal('samples', lang="en")))
        new_graph.add((getattr(NGO, subject_PID), RDF.type, SCI.S13_Sample))
        new_graph.add((getattr(NGO, subject_PID), CRM.P2_has_type, SCI.S13_Sample))
        new_graph.add((getattr(NGO, sampling_event_PID), CRM.P14_carried_out_by, getattr(NGO, national_gallery_staff_member_PID)))
        new_graph.add((getattr(NGO, national_gallery_staff_member_PID), RDF.type, CRM.E21_Person))
        new_graph.add((getattr(NGO, national_gallery_staff_member_PID), CRM.P2_has_type, CRM.E21_Person))
        new_graph.add((getattr(NGO, national_gallery_staff_member_PID), CRM.P2_has_type, getattr(AAT, '300025820')))
        new_graph.add((getattr(AAT, '300025820'), RDFS.label, Literal('conservation scientist', lang="en")))
        new_graph.add((getattr(NGO, national_gallery_staff_member_PID), RDFS.label, Literal('Example NG Staff Member', lang="en")))

        new_graph.add((getattr(NGO, sampled_section), RDF.type, CRM.E53_Place))
        new_graph.add((getattr(NGO, sampled_section), CRM.P2_has_type, CRM.E53_Place))
        new_graph.add((getattr(NGO, sampled_section), CRM.P2_has_type, getattr(AAT, '300241583')))
        new_graph.add((getattr(AAT, '300241583'), RDFS.label, Literal('components (object part)', lang="en")))
        new_graph.add((getattr(NGO, sampling_event_PID), SCI.O4_sampled_at, getattr(NGO, sampled_section)))
        new_graph.add((getattr(NGO, sampled_section), RDFS.label, Literal(sample_site_label, lang="en")))

        new_graph.add((getattr(NGO, sampling_event_PID), CRM.P70_is_documented_in, doc_BN))
        new_graph.add((doc_BN, RDF.type, CRM.E31_Document))
        new_graph.add((doc_BN, CRM.P2_has_type, CRM.E31_Document))
        new_graph.add((doc_BN, CRM.P2_has_type, getattr(WD, 'Q1710397')))
        new_graph.add((getattr(WD, 'Q1710397'), RDFS.label, Literal('reason', lang="en")))
        new_graph.add((doc_BN, RDFS.comment, Literal('Actual textual content detailing the reason or purpose of the annotation', lang="en")))
        if kwargs["sample_comment"] is not None and kwargs["sample_comment"] != '':
            new_graph.add((getattr(NGO, sampling_event_PID), RDFS.comment, Literal(kwargs["sample_comment"], lang="en")))
        else:
            new_graph.add((getattr(NGO, sampling_event_PID), RDFS.comment, Literal('Actual textual content describing the sampling process', lang="en")))

        if kwargs["sample_type"] == 'cross section':
            new_graph.add((getattr(NGO, subject_PID), CRM.P2_has_type, getattr(AAT, '300034254')))
            new_graph.add((getattr(AAT, '300034254'), RDFS.label, Literal('cross sections (orthographic projections)', lang="en")))

        if kwargs["object_inventory_number"] is not None and kwargs["object_inventory_number"] != '':
            new_graph.add((getattr(NGO, subject_PID), RDFS.label, Literal('Sample from ' + kwargs["object_inventory_number"], lang="en")))

        new_graph.add((getattr(NGO, sampled_work_PID), CRM.P59_has_section, getattr(NGO, sampled_section)))
        new_graph.add((getattr(NGO, sampling_event_PID), SCI.O3_sampled_from, getattr(NGO, sampled_work_PID)))
        new_graph.add((getattr(NGO, subject_PID), SCI.O25i_is_contained_in, getattr(NGO, sampled_work_PID)))

    elif kwargs["event_type"] == 'sample preparation':
        new_graph.add((getattr(NGO, subject_PID), SCI.O18i_was_altered_by, getattr(NGO, sampling_event_PID)))
        new_graph.add((getattr(NGO, sampling_event_PID), RDF.type, SCI.S18_Alteration))
        new_graph.add((getattr(NGO, sampling_event_PID), CRM.P2_has_type, SCI.S18_Alteration))
        new_graph.add((getattr(NGO, sampling_event_PID), CRM.P2_has_type, getattr(AAT, '300443464')))
        new_graph.add((getattr(AAT, '300443464'), RDFS.label, Literal('embedding', lang="en")))
        new_graph.add((getattr(NGO, sampling_event_PID), CRM.P2_has_type, CRM.E13_Attribute_Assignment))
        new_graph.add((getattr(NGO, sampling_event_PID), CRM.P140_assigned_attribute_to, getattr(NGO, subject_PID)))
        new_graph.add((getattr(NGO, sampling_event_PID), CRM.P177_assigned_property_of_type, getattr(AAT, '300034254')))
        new_graph.add((getattr(AAT, '300034254'), RDFS.label, Literal('cross sections', lang="en")))
        new_graph.add((getattr(NGO, sampling_event_PID), RDFS.label, Literal(sampling_event, lang="en")))

    elif kwargs["event_type"] == 'sample image acquisition':
        event_PID = generate_placeholder_PID(kwargs["event_name"])
        image_name = kwargs["event_name"].replace('acquisition of sample image ', '')
        image_PID = generate_placeholder_PID(image_name)

        new_graph.add((getattr(NGO, subject_PID), CRM.P12i_was_present_at, getattr(NGO, event_PID)))
        new_graph.add((getattr(NGO, event_PID), RDF.type, CRM.E12_Production))
        new_graph.add((getattr(NGO, event_PID), CRM.P2_has_type, CRM.E12_Production))
        new_graph.add((getattr(NGO, event_PID), RDFS.label, Literal(kwargs["event_name"], lang="en")))
        new_graph.add((getattr(NGO, event_PID), CRM.P108_has_produced, getattr(NGO, image_PID)))
        new_graph.add((getattr(NGO, image_PID), RDF.type, CRM.E24_Physical_Human_Made_Thing))
        new_graph.add((getattr(NGO, image_PID), CRM.P2_has_type, CRM.E24_Physical_Human_Made_Thing))
        new_graph.add((getattr(NGO, image_PID), RDFS.label, Literal(image_name, lang="en")))

    return new_graph

def create_sample_layer_triples(new_graph, **kwargs):
    layer_PID = kwargs["layer_PID"]
    sample_PID = kwargs["sample_PID"]
    object_PID = kwargs["object_PID"]
    sample_layer_number = 'Layer ' + kwargs["sample_layer_number"] + ' of sample ' + kwargs["sample_number"] + ' from ' + kwargs["object_inventory_number"]
    object_layer_number = 'Layer ' + kwargs["object_layer_number"] + ' of ' + kwargs["object_inventory_number"]

    new_graph.add((getattr(NGO, sample_PID), CRM.P148_has_component, getattr(NGO, layer_PID)))
    new_graph.add((getattr(NGO, layer_PID), RDF.type, SCI.S22_Segment_of_Matter))
    new_graph.add((getattr(NGO, layer_PID), CRM.P2_has_type, SCI.S22_Segment_of_Matter))
    new_graph.add((getattr(NGO, layer_PID), CRM.P48_has_preferred_identifier, Literal(sample_layer_number, lang="en")))
    new_graph.add((getattr(NGO, layer_PID), RDFS.label, Literal(sample_layer_number, lang="en")))
    new_graph.add((getattr(NGO, layer_PID), CRM.P1_is_identified_by, Literal(object_layer_number, lang="en")))
    new_graph.add((getattr(NGO, layer_PID), CRM.P2_has_type, getattr(AAT, '300226788')))
    new_graph.add((getattr(AAT, '300226788'), RDFS.label, Literal('layers (components)', lang="en")))

    if kwargs["sample_layer_comment"] is not None and kwargs["sample_layer_comment"] != '':
        new_graph.add((getattr(NGO, layer_PID), CRM.P3_has_note, Literal(kwargs["sample_layer_comment"], lang="en")))

    return new_graph

def create_sample_component_triples(new_graph, **kwargs):
    layer_PID = kwargs["layer_PID"]
    sample_PID = kwargs["sample_PID"]
    component_PID = kwargs["component_PID"]
    try: component_number = 'Component ' + kwargs["sample_component_number"] + ' of sample ' + kwargs["sample_number"] + ' from ' + kwargs["object_inventory_number"]
    except: st()
    function_bn = BNode()
    dimension_BN = BNode()
    size_BN = BNode()

    new_graph.add((getattr(NGO, layer_PID), SCI.O25_contains, getattr(NGO, component_PID)))
    new_graph.add((getattr(NGO, component_PID), RDF.type, SCI.S22_Segment_of_Matter))
    new_graph.add((getattr(NGO, component_PID), CRM.P2_has_type, SCI.S22_Segment_of_Matter))
    new_graph.add((getattr(NGO, component_PID), RDFS.label, Literal(component_number, lang="en")))

    if kwargs["sample_component_function"] is not None and kwargs["sample_component_function"] != '':
        new_graph.add((getattr(NGO, component_PID), CRM.P101_had_as_general_use, function_bn))
        new_graph.add((function_bn, RDFS.label, Literal(kwargs["sample_component_function"], lang="en")))

    if kwargs["sample_comp_function_confidence"] is not None and kwargs["sample_comp_function_confidence"] != '':
        
        new_graph.add((function_bn, RDFS.label, getattr(NGO, kwargs["sample_comp_function_confidence"])))

    if kwargs["sample_component_comment"] is not None and kwargs["sample_component_comment"] != '':

        new_graph.add((getattr(NGO, component_PID), RDFS.comment, Literal(kwargs["sample_component_comment"], lang="en")))

    if kwargs["sample_component_amount"] is not None and kwargs["sample_component_amount"] != '':
        if kwargs["sample_component_amount"] == 'major':
            sample_component_size = 'Major'
        elif kwargs["sample_component_amount"] == 'minor':
            sample_component_size = 'Minor'
        elif kwargs["sample_component_amount"] == 'occasional':
            sample_component_size = 'Occasional'
        new_graph.add((getattr(NGO, component_PID), CRM.P43_has_dimension, dimension_BN))
        new_graph.add((dimension_BN, CRM.P2_has_type, getattr(WD, 'Q309314')))
        new_graph.add((getattr(WD, 'Q309314'), RDFS.label, Literal('quantity', lang="en")))
        new_graph.add((dimension_BN, CRM.P2_has_type, getattr(NGO, sample_component_size)))
        new_graph.add((dimension_BN, RDF.type, CRM.E54_Dimension))

    if kwargs["sample_component_size"] is not None and kwargs["sample_component_size"] != '':
        new_graph.add((getattr(NGO, component_PID), CRM.P43_has_dimension, size_BN))
        new_graph.add((size_BN, CRM.P2_has_type, getattr(AAT, '300266035')))
        new_graph.add((getattr(AAT, '300266035'), RDFS.label, Literal('size/dimension', lang="en")))
        new_graph.add((size_BN, RDF.type, CRM.E54_Dimension))
        if kwargs["sample_component_size"].find('coarse') != -1:
            new_graph.add((size_BN, CRM.P2_has_type, getattr(AAT, '300014650')))
            new_graph.add((getattr(AAT, '300014650'), RDFS.label, Literal('coarse-grain material', lang="en")))
        elif kwargs["sample_component_size"].find('fine') != -1:
            new_graph.add((size_BN, CRM.P2_has_type, getattr(AAT, '300014645')))
            new_graph.add((getattr(AAT, '300014645'), RDFS.label, Literal('fine-grain material', lang="en")))
        elif kwargs["sample_component_size"].find('medium') != -1:
            new_graph.add((size_BN, CRM.P2_has_type, getattr(AAT, '300014648')))
            new_graph.add((getattr(AAT, '300014648'), RDFS.label, Literal('medium-grain material', lang="en")))

    return new_graph

def create_colour_triples(new_graph, **kwargs):    
    object_PID = kwargs["object_PID"]
    colour_BN = BNode()
    colour_label = 'colour of ' + kwargs["object_PID"]

    new_graph.add((getattr(NGO, object_PID), CRM.P56_bears_feature, colour_BN))
    new_graph.add((colour_BN, RDF.type, CRM.E26_Physical_Feature))
    new_graph.add((colour_BN, CRM.P2_has_type, CRM.E26_Physical_Feature))
    new_graph.add((colour_BN, CRM.P2_has_type, getattr(AAT, '300056135')))
    new_graph.add((getattr(AAT, '300056135'), RDFS.label, Literal('surface colour', lang="en")))
    new_graph.add((colour_BN, CRM.P1_is_identified_by, getattr(NGO, kwargs["main_colour_PID"])))
    new_graph.add((colour_BN, CRM.P1_is_identified_by, getattr(NGO, kwargs["descriptor_colour_PID"])))
    new_graph.add((getattr(NGO, kwargs["main_colour_PID"]), RDFS.label, Literal(kwargs["main_colour_name"], lang="en")))
    if kwargs["main_colour_aat_value"] is not None and kwargs["main_colour_aat_value"] != '':
        new_graph.add((getattr(NGO, kwargs["main_colour_PID"]), CRM.P1_is_identified_by, getattr(AAT, kwargs["main_colour_aat_value"])))
        new_graph.add((getattr(NGO, kwargs["main_colour_PID"]), CRM.P2_has_type, getattr(NGO, 'Main_Colour')))
        new_graph.add((getattr(AAT, kwargs["main_colour_aat_value"]), RDFS.label, Literal(kwargs["main_colour_aat_title"], lang="en")))
    if kwargs["main_colour_comment"] is not None and kwargs["main_colour_comment"] != '':
        new_graph.add((getattr(NGO, kwargs["main_colour_PID"]), CRM.P3_has_note, Literal(kwargs["main_colour_comment"], lang="en")))
    if kwargs["modifier_colour_PID"] is not None:
        new_graph.add((colour_BN, CRM.P1_is_identified_by, getattr(NGO, kwargs["modifier_colour_PID"])))
        new_graph.add((getattr(NGO, kwargs["modifier_colour_PID"]), RDFS.label, Literal(kwargs["modifier_colour_name"], lang="en")))
    if kwargs["modifier_colour_aat_value"] is not None and kwargs["modifier_colour_aat_value"] != '':
        new_graph.add((getattr(NGO, kwargs["modifier_colour_PID"]), CRM.P1_is_identified_by, getattr(AAT, kwargs["modifier_colour_aat_value"])))
        new_graph.add((getattr(NGO, kwargs["main_colour_PID"]), CRM.P2_has_type, getattr(NGO, 'Modifier_Colour')))
        new_graph.add((getattr(AAT, kwargs["modifier_colour_aat_value"]), RDFS.label, Literal(kwargs["modifier_colour_aat_title"], lang="en")))
    if kwargs["modifier_colour_comment"] is not None and kwargs["modifier_colour_comment"] != '':
        new_graph.add((getattr(NGO, kwargs["modifier_colour_PID"]), CRM.P3_has_note, Literal(kwargs["modifier_colour_comment"], lang="en")))
    new_graph.add((getattr(NGO, kwargs["descriptor_colour_PID"]), RDFS.label, Literal(kwargs["descriptor_colour_name"], lang="en")))
    if kwargs["descriptor_colour_aat_value"] is not None and kwargs["descriptor_colour_aat_value"] != '':
        new_graph.add((getattr(NGO, kwargs["descriptor_colour_PID"]), CRM.P1_is_identified_by, getattr(AAT, kwargs["descriptor_colour_aat_value"])))
        new_graph.add((getattr(NGO, kwargs["main_colour_PID"]), CRM.P2_has_type, getattr(NGO, 'Colour_Descriptor')))
        new_graph.add((getattr(AAT, kwargs["descriptor_colour_aat_value"]), RDFS.label, Literal(kwargs["descriptor_colour_aat_title"], lang="en")))
    if kwargs["descriptor_colour_comment"] is not None and kwargs["descriptor_colour_comment"] != '':
        new_graph.add((getattr(NGO, kwargs["descriptor_colour_PID"]), CRM.P3_has_note, Literal(kwargs["descriptor_colour_comment"], lang="en")))

    return new_graph

def create_preparation_triples(new_graph, **kwargs):

    object_PID = kwargs["object_PID"]
    preparation_PID = kwargs["prep_PID"]
    prep_name = kwargs["prep_name"]
    prep_layer_PID = kwargs["prep_layer_PID"]

    new_graph.add((getattr(NGO, preparation_PID), CRM.P31_has_modified, getattr(NGO, prep_layer_PID)))
    new_graph.add((getattr(NGO, prep_layer_PID), CRM.P2_has_type, CRM.E25_Human_Made_Feature))
    new_graph.add((getattr(NGO, prep_layer_PID), RDF.type, CRM.E25_Human_Made_Feature))
    new_graph.add((getattr(NGO, prep_layer_PID), RDFS.label, Literal(kwargs["prep_layer_name"], lang="en")))
    new_graph.add((getattr(NGO, object_PID), CRM.P56_bears_feature, getattr(NGO, prep_layer_PID)))
    new_graph.add((getattr(NGO, preparation_PID), RDF.type, CRM.E11_Modification))
    new_graph.add((getattr(NGO, preparation_PID), CRM.P2_has_type, CRM.E11_Modification))
    new_graph.add((getattr(NGO, preparation_PID), RDFS.label, Literal(prep_name, lang="en")))

    if kwargs["application_technique"] is not None and kwargs["application_technique"] != '':
        application_technique = generate_placeholder_PID(kwargs["application_technique"])

        new_graph.add((getattr(NGO, preparation_PID), CRM.P32_used_general_technique, getattr(NGO, application_technique)))
        new_graph.add((getattr(NGO, application_technique, RDF.type, CRM.E55_Type)))
        new_graph.add((getattr(NGO, application_technique, CRM.P2_has_type, CRM.E55_Type)))
        new_graph.add((getattr(NGO, application_technique, RDFS.label, Literal(kwargs["application_technique"], lang="en"))))

    if kwargs["preparation_comment"] is not None and kwargs["preparation_comment"] != '':
        new_graph.add((getattr(NGO, preparation_PID), RDFS.comment, Literal(kwargs["preparation_comment"], lang="en")))

    return new_graph

def create_protocol_triples(new_graph, **kwargs):

    document_PID = kwargs["document_PID"]
    event_PID = kwargs["event_PID"]
    institution_PID = kwargs["institution_PID"]
    aat_dict = find_aat_value(kwargs["protocol_type"], 'protocols')
    file_name = BNode()
    technique_PID = kwargs["technique_PID"]

    new_graph.add((getattr(NGO, document_PID), CRM.P70_documents, getattr(NGO, event_PID)))
    new_graph.add((getattr(NGO, document_PID), RDF.type, CRM.E31_Document))
    new_graph.add((getattr(NGO, document_PID), CRM.P2_has_type, CRM.E31_Document))
    new_graph.add((getattr(NGO, document_PID), RDFS.label, Literal(kwargs["protocol_name"], lang="en")))
    new_graph.add((getattr(NGO, document_PID), CRM.P2_has_type, getattr(AAT, '300027042')))
    new_graph.add((getattr(AAT, '300027042'), RDFS.label, Literal('instructions (document genre)', lang="en")))
    for number, title in aat_dict.items():
        new_graph.add((getattr(NGO, document_PID), CRM.P2_has_type, getattr(AAT, number)))
        new_graph.add((getattr(AAT, number), RDFS.label, Literal(title, lang="en")))
    new_graph.add((getattr(NGO, document_PID), CRM.P1_is_identified_by, Literal(kwargs["protocol_type"], lang="en")))
    new_graph.add((getattr(NGO, document_PID), CRM.P149_is_identified_by, file_name))
    new_graph.add((file_name, RDF.type, CRM.E42_Identifier))
    new_graph.add((file_name, CRM.P2_has_type, CRM.E42_Identifier))
    new_graph.add((file_name, CRM.P2_has_type, getattr(WD, 'Q1144928')))
    new_graph.add((getattr(WD, 'Q1144928'), RDFS.label, Literal('filename', lang="en")))
    new_graph.add((file_name, RDFS.label, Literal(kwargs["protocol_file"], lang="en")))
    new_graph.add((getattr(NGO, event_PID), CRM.P7_took_place_at, getattr(NGO, institution_PID)))

    if kwargs["protocol_comment"] is not None and kwargs["protocol_comment"] != '':
        new_graph.add((getattr(NGO, document_PID), RDFS.comment, Literal(kwargs["protocol_comment"], lang="en")))

    new_graph.add((getattr(NGO, event_PID), CRM.P32_used_general_technique, getattr(NGO, technique_PID)))
    new_graph.add((getattr(NGO, technique_PID), RDFS.label, Literal(kwargs["technique_name"], lang="en")))
    new_graph.add((getattr(NGO, technique_PID), CRM.P48_has_preferred_identifier, Literal(kwargs["technique_name"], lang="en")))

    if kwargs["technique_full_name"] is not None and kwargs["technique_full_name"] != '':
        new_graph.add((getattr(NGO, technique_PID), CRM.P1_is_identified_by, Literal(kwargs["technique_full_name"], lang="en")))

    if kwargs["technique_comment"] is not None and kwargs["technique_comment"] != '':
        new_graph.add((getattr(NGO, technique_PID), RDFS.comment, Literal(kwargs["technique_comment"], lang="en")))

    if kwargs["technique_link"] is not None and kwargs["technique_link"] != '':
        website_PID = BNode()

        new_graph.add((getattr(NGO, technique_PID), CRM.P70i_is_documented_in, website_PID))
        new_graph.add((website_PID, RDF.type, CRM.E73_Information_Object))
        new_graph.add((website_PID, CRM.P2_has_type, CRM.E73_Information_Object))
        new_graph.add((website_PID, RDFS.label, Literal(kwargs["technique_link"], lang="en")))
        new_graph.add((website_PID, CRM.P2_has_type, getattr(AAT, '300265431')))
        new_graph.add((getattr(AAT, '300265431'), RDFS.label, Literal('Web sites', lang="en")))
    
    if kwargs["technique_name"] is not None and kwargs["technique_name"] != '':
        technique_aat_dict = find_aat_value(kwargs["technique_name"], 'techniques')
        for number, title in technique_aat_dict.items():
            new_graph.add((getattr(NGO, technique_PID), CRM.P2_has_type, getattr(AAT, number)))
            new_graph.add((getattr(AAT, number), RDFS.label, Literal(title, lang="en")))

    return new_graph

def create_measurement_triples(new_graph, **kwargs):

    measurement_event = kwargs["event_PID"]
    object_id = kwargs["object_inventory_number"]
    related_painting_PID = generate_placeholder_PID(object_id)
    material_PID = generate_placeholder_PID(kwargs["material_name"])
    dimension_BN = BNode()
    aat_dict = find_aat_value(kwargs["material_name"], 'material_grounds')

    new_graph.add((getattr(NGO, measurement_event), RDFS.label, Literal(kwargs["event_name"], lang="en")))
    new_graph.add((getattr(NGO, measurement_event), RDF.type, CRM.E16_Measurement))
    new_graph.add((getattr(NGO, measurement_event), CRM.P2_has_type, CRM.E16_Measurement))
    new_graph.add((getattr(NGO, measurement_event), CRM.P39_measured, getattr(NGO, material_PID)))
    new_graph.add((getattr(NGO, measurement_event), CRM.P12_occurred_in_the_presence_of, getattr(NGO, related_painting_PID)))
    new_graph.add((getattr(NGO, measurement_event), CRM.P32_used_general_technique, getattr(AAT, '300053578')))
    new_graph.add((getattr(AAT, '300053578'), RDFS.label, Literal('measuring', lang="en")))
    new_graph.add((getattr(NGO, measurement_event), CRM.P40_observed_dimension, dimension_BN))
    new_graph.add((dimension_BN, RDF.type, CRM.E54_Dimension))
    new_graph.add((dimension_BN, CRM.P2_has_type, CRM.E54_Dimension))
    new_graph.add((dimension_BN, RDFS.label, Literal('quantity of ' + kwargs['material_name'])))

    if kwargs["material_value"] is not None and kwargs["material_value"] != '':
        new_graph.add((dimension_BN, CRM.P90_has_value, Literal(kwargs["material_value"])))

    if kwargs["material_value_percent"] is not None and kwargs["material_value_percent"] != '':
        new_graph.add((dimension_BN, CRM.P90_has_value, Literal(kwargs["material_value_percent"])))

    if kwargs["measurement_comment"] is not None and kwargs["measurement_comment"] != '':
        new_graph.add((getattr(NGO, measurement_event), RDFS.comment, Literal(kwargs["measurement_comment"], lang="en")))

    if kwargs["measurementXcomposition_comment"] is not None and kwargs["measurementXcomposition_comment"] != '':
        new_graph.add((dimension_BN, RDFS.comment, Literal(kwargs["measurementXcomposition_comment"], lang="en")))

    for number, title in aat_dict.items():
        new_graph.add((getattr(NGO, material_PID), CRM.P2_has_type, getattr(AAT, number)))
        new_graph.add((getattr(AAT, number), RDFS.label, Literal(title, lang="en")))

    if kwargs["result_confidence"] is not None and kwargs["result_confidence"] != '':
        new_graph.add((getattr(NGO, material_PID), RDFS.label, getattr(NGO, kwargs["result_confidence"])))

    if kwargs["material_type"] == 'chemical compound':
        new_graph.add((getattr(NGO, material_PID), CRM.P2_has_type, getattr(AAT, '300246922')))
        new_graph.add((getattr(AAT, '300246922'), RDFS.label, Literal('compounds (materials)', lang="en")))

    if kwargs["material_comment"] is not None and kwargs["material_comment"] != '':
        new_graph.add((getattr(NGO, material_PID), RDFS.comment, Literal(kwargs["material_comment"], lang="en")))

    if kwargs["material_link"] is not None and kwargs["material_link"] != '':
        website_PID = BNode()

        new_graph.add((getattr(NGO, material_PID), CRM.P70i_is_documented_in, website_PID))
        new_graph.add((website_PID, RDF.type, CRM.E73_Information_Object))
        new_graph.add((website_PID, CRM.P2_has_type, CRM.E73_Information_Object))
        new_graph.add((website_PID, RDFS.label, Literal(kwargs["material_link"], lang="en")))
        new_graph.add((website_PID, CRM.P2_has_type, getattr(AAT, '300265431')))
        new_graph.add((getattr(AAT, '300265431'), RDFS.label, Literal('Web sites', lang="en")))

    if kwargs["material_class"] is not None and kwargs["material_class"] != '':
        class_BN = BNode()

        new_graph.add((getattr(NGO, material_PID), CRM.P2_has_type, class_BN))
        new_graph.add((class_BN, RDF.type, CRM.E55_Type))
        new_graph.add((class_BN, RDFS.label, Literal(kwargs["material_class"], lang="en")))

        if kwargs["material_subclass"] is not None and kwargs["material_subclass"] != '':
            subclass_BN = BNode()

            new_graph.add((class_BN, CRM.P2_has_type, subclass_BN))
            new_graph.add((subclass_BN, RDF.type, CRM.E55_Type))
            new_graph.add((subclass_BN, RDFS.label, Literal(kwargs["material_subclass"], lang="en")))

    return new_graph

def create_location_triples(new_graph, **kwargs):
    sampled_section = create_PID_from_triple(kwargs["object_part_name"], kwargs["object_inventory_number"])
    location_PID = kwargs["location_PID"]
    object_PID = generate_placeholder_PID(kwargs["object_inventory_number"])
    doc_BN = BNode()
    object_side = BNode()

    new_graph.add((getattr(NGO, sampled_section), CRM.P161_has_spatial_projection, getattr(NGO, location_PID)))
    new_graph.add((getattr(NGO, location_PID), RDF.type, CRM.E53_Place))
    new_graph.add((getattr(NGO, location_PID), CRM.P2_has_type, CRM.E53_Place))
    new_graph.add((getattr(NGO, location_PID), RDFS.label, Literal(kwargs["location_name"], lang="en")))
    new_graph.add((getattr(NGO, location_PID), CRM.P70_is_documented_in, doc_BN))
    new_graph.add((doc_BN, RDF.type, CRM.E31_Document))
    new_graph.add((doc_BN, CRM.P2_has_type, CRM.E31_Document))
    new_graph.add((doc_BN, CRM.P2_has_type, getattr(AAT, '300411780')))
    new_graph.add((getattr(AAT, '300411780'), RDFS.label, Literal('descriptions (documents)', lang="en")))
    new_graph.add((doc_BN, RDFS.comment, Literal(kwargs["location_description"], lang="en")))
    if kwargs["location_type"] == 'sample':
        new_graph.add((getattr(NGO, location_PID), CRM.P2_has_type, getattr(AAT, '300178447')))
        new_graph.add((getattr(AAT, '300178447'), RDFS.label, Literal('painting components', lang="en")))
    if kwargs["object_side"] is not None and kwargs["object_side"] != '':
        new_graph.add((getattr(NGO, object_PID), CRM.P46_is_composed_of, object_side))
        new_graph.add((object_side, RDFS.label, Literal(kwargs["object_side"], lang="en")))
        new_graph.add((object_side, RDF.type, CRM.E26_Physical_Feature))
        new_graph.add((object_side, CRM.P2_has_type, CRM.E26_Physical_Feature))
        new_graph.add((object_side, CRM.P59_has_section, getattr(NGO, location_PID)))
    if kwargs["location_comment"] is not None and kwargs["location_comment"] != '':
        new_graph.add((getattr(NGO, location_PID), RDFS.comment, Literal(kwargs["location_comment"], lang="en")))
    new_graph.add((getattr(NGO, object_PID, CRM.P59_has_section, getattr(NGO, location_PID))))

    if kwargs["image_location_x"] is not None and kwargs["image_location_y"] is not None:
        image_coordinates = "(" + kwargs["image_location_x"] + ", " + kwargs["image_location_y"] + ")"
        new_graph.add((getattr(location_PID), CRM.P168_place_is_defined_by, Literal(image_coordinates)))
    if kwargs["model_location_x"] is not None and kwargs["model_location_x"] is not None and kwargs["model_location_z"] is not None:
        image_coordinates = "(" + kwargs["model_location_x"] + ", " + kwargs["model_location_x"] + ", " + kwargs["model_location_z"] + ")"
        new_graph.add((getattr(location_PID), CRM.P168_place_is_defined_by, Literal(image_coordinates)))

    if kwargs["image_file"] is not None:
        image_file_PID = generate_placeholder_PID(kwargs["image_file"])
        new_graph.add((getattr(NGO, location_PID), CRM.P62i_is_depicted_by, getattr(NGO, image_file_PID)))
    elif kwargs["mesh_name"] is not None:
        model_file_PID = generate_placeholder_PID(kwargs["mesh_name"])
        new_graph.add((getattr(NGO, location_PID), CRM.P62i_is_depicted_by, getattr(NGO, image_file_PID)))

    return new_graph

def create_image_file_triples(new_graph, **kwargs):
    file_PID = kwargs["image_PID"]
    related_work = kwargs["object_PID"]
    doc_BN = BNode()
    creation_event_PID = create_PID_from_triple('creation', kwargs["image_file"])

    new_graph.add((getattr(NGO, file_PID), RDF.type, CRM.E24_Physical_Human_Made_Thing))
    new_graph.add((getattr(NGO, file_PID), CRM.P2_has_type, CRM.E24_Physical_Human_Made_Thing))
    new_graph.add((getattr(NGO, file_PID), CRM.P108i_was_produced_by, getattr(NGO, creation_event_PID)))
    new_graph.add((getattr(NGO, creation_event_PID), RDF.type, CRM.E12_Production))
    new_graph.add((getattr(NGO, creation_event_PID), CRM.P2_has_type, CRM.E12_Production))
    new_graph.add((getattr(NGO, creation_event_PID), CRM.P2_has_type, getattr(AAT, '300404387')))
    new_graph.add((getattr(AAT, '300404387'), RDFS.label, Literal('creating (artistic activity)', lang="en")))

    if kwargs["image_lightsource"] is not None and kwargs["image_lightsource"] != '':
        if kwargs["image_lightsource"] == 'Visible Light' or kwargs["image_lightsource"] == 'Visible':
            aat_ref = '300054225'
            aat_name = 'photography (process)'
        elif kwargs["image_lightsource"] == 'Infrared':
            aat_ref = '300053463'
            aat_name = 'infrared photography'
        elif kwargs["image_lightsource"] == 'Ultra-violet':
            aat_ref = '300053465'
            aat_name = 'ultraviolet photography'
        elif kwargs["image_lightsource"] == 'X-ray':
            aat_ref = '300053450'
            aat_name = 'radiography'
        else:
            aat_ref = None
            aat_name = None
        
        if aat_ref is not None:
            new_graph.add((getattr(NGO, creation_event_PID), CRM.P32_used_general_technique, getattr(AAT, aat_ref)))
            new_graph.add((getattr(AAT, aat_ref), RDFS.label, Literal(aat_name, lang="en")))

    if kwargs["image_optical_spec"] is not None and kwargs["image_optical_spec"] != '':
        if kwargs["image_optical_spec"] == 'dark field' or kwargs["image_optical_spec"] == 'DF':
            aat_ref = '300179535'
            aat_name = 'spectroscopy'
        elif kwargs["image_optical_spec"] == 'UV-A' or kwargs["image_optical_spec"] == 'UV-I3':
            aat_ref = '300375717'
            aat_name = 'ultraviolet-visible spectroscopy'
        elif kwargs["image_optical_spec"] == 'fluorescence':
            aat_ref = '300379945'
            aat_name = 'fluorimetry'
        else:
            aat_ref is None
            aat_name is None
        
        if aat_ref is not None:
            new_graph.add((getattr(NGO, creation_event_PID), CRM.P32_used_general_technique, getattr(AAT, aat_ref)))
            new_graph.add((getattr(AAT, aat_ref), RDFS.label, Literal(aat_name, lang="en")))

    if kwargs["image_filedate"] is not None and kwargs["image_filedate"] != '':
        image_filedate = BNode()
        image_timespan = create_PID_from_triple('timespan', creation_event_PID)

        new_graph.add((getattr(NGO, creation_event_PID), CRM.P4_has_time_span, getattr(NGO, image_timespan)))
        new_graph.add((getattr(NGO, image_timespan), RDF.type, CRM.E52_Time_span))
        new_graph.add((getattr(NGO, image_timespan), CRM.P2_has_type, CRM.E52_Time_span))
        new_graph.add((getattr(NGO, image_timespan), CRM.P82_at_some_time_within, image_filedate))
        new_graph.add((image_filedate, RDF.type, CRM.E61_Time_Primitive))
        new_graph.add((image_filedate, CRM.P2_has_type, CRM.E61_Time_Primitive))
        new_graph.add((image_filedate, RDFS.label, Literal(kwargs["image_filedate"])))

    new_graph.add((getattr(NGO, file_PID), RDF.type, CRM.E73_Information_Object))
    new_graph.add((getattr(NGO, file_PID), CRM.P2_has_type, CRM.E73_Information_Object))
    new_graph.add((getattr(NGO, file_PID), RDFS.label, Literal(kwargs["image_name"], lang="en")))

    if kwargs["image_file"] is not None and kwargs["image_file"] != '':
        file_name = BNode()
        new_graph.add((getattr(NGO, file_PID), CRM.P149_is_identified_by, file_name))
        new_graph.add((file_name, RDF.type, CRM.E42_Identifier))
        new_graph.add((file_name, CRM.P2_has_type, CRM.E42_Identifier))
        new_graph.add((file_name, CRM.P2_has_type, getattr(WD, 'Q1144928')))
        new_graph.add((getattr(WD, 'Q1144928'), RDFS.label, Literal('filename', lang="en")))
        new_graph.add((file_name, RDFS.label, Literal(kwargs["image_file"], lang="en")))
        new_graph.add((getattr(NGO, file_PID), RDF.type, DIG.D1_Digital_Object))
        new_graph.add((getattr(NGO, file_PID), CRM.P2_has_type, DIG.D1_Digital_Object))

    elif kwargs["image_format"] is not None and kwargs["image_format"] != '':
        if kwargs["image_format"] == 'PDF':
            new_graph.add((getattr(NGO, file_PID), CRM.P2_has_type, getattr(AAT, '300266022')))
            new_graph.add((getattr(AAT, '300266022'), RDFS.label, Literal('PDF', lang="en")))
        elif kwargs["image_format"] == 'JPEG':
            new_graph.add((getattr(NGO, file_PID), CRM.P2_has_type, getattr(AAT, '300266224')))
            new_graph.add((getattr(AAT, '300266224'), RDFS.label, Literal('JPEG', lang="en")))
        elif kwargs["image_format"] == 'TIFF':
            new_graph.add((getattr(NGO, file_PID), CRM.P2_has_type, getattr(AAT, '300266226')))
            new_graph.add((getattr(AAT, '300266226'), RDFS.label, Literal('TIFF', lang="en")))

    if kwargs["image_height"] is not None and kwargs["image_height"] != '':
        height_PID = create_PID_from_triple('height', kwargs["image_file"])
        new_graph.add((getattr(NGO, kwargs["image_PID"]), CRM.P43_has_dimension, getattr(NGO, height_PID)))
        new_graph.add((getattr(NGO, height_PID), RDF.type, CRM.E54_Dimension))
        new_graph.add((getattr(NGO, height_PID), CRM.P90_has_value, Literal(kwargs["image_height"], datatype=XSD.double)))
        new_graph.add((getattr(NGO, height_PID), CRM.P91_has_unit, getattr(AAT,'300379098')))
        new_graph.add((getattr(AAT, '300379098'), RDFS.label, Literal('centimeters', lang="en")))
        new_graph.add((getattr(NGO, height_PID), RDF.type, getattr(AAT, '300055644')))
        new_graph.add((getattr(AAT, '300055644'), RDFS.label, Literal('height', lang="en")))

    if kwargs["image_width"] is not None and kwargs["image_width"] != '':
        width_PID = create_PID_from_triple('width', kwargs["image_file"])
        new_graph.add((getattr(NGO, kwargs["image_PID"]), CRM.P43_has_dimension, getattr(NGO, width_PID)))
        new_graph.add((getattr(NGO, width_PID), RDF.type, CRM.E54_Dimension))
        new_graph.add((getattr(NGO, width_PID), CRM.P90_has_value, Literal(kwargs["image_width"], datatype=XSD.double)))
        new_graph.add((getattr(NGO, width_PID), CRM.P91_has_unit, getattr(AAT,'300379098')))
        new_graph.add((getattr(AAT, '300379098'), RDFS.label, Literal('centimeters', lang="en")))
        new_graph.add((getattr(NGO, width_PID), RDF.type, getattr(AAT, '300055647')))
        new_graph.add((getattr(AAT, '300055647'), RDFS.label, Literal('width', lang="en")))

    if kwargs["image_ppmm"] is not None and kwargs["image_ppmm"] != '':
        ppmm_PID = create_PID_from_triple('resolution', kwargs["image_file"])
        ppmm = kwargs["image_ppmm"] * 10
        new_graph.add((getattr(NGO, kwargs["image_PID"]), CRM.P43_has_dimension, getattr(NGO, ppmm_PID)))
        new_graph.add((getattr(NGO, ppmm_PID), RDF.type, CRM.E54_Dimension))
        new_graph.add((getattr(NGO, ppmm_PID), CRM.P90_has_value, Literal(ppmm, datatype=XSD.double)))
        new_graph.add((getattr(NGO, ppmm_PID), CRM.P91_has_unit, getattr(AAT,'300412058')))
        new_graph.add((getattr(AAT, '300412058'), RDFS.label, Literal('pixels per centimeter', lang="en")))
        new_graph.add((getattr(NGO, ppmm_PID), RDF.type, getattr(AAT, '300137851')))
        new_graph.add((getattr(AAT, '300137851'), RDFS.label, Literal('resolution (function)', lang="en")))

    if kwargs["image_levels"] is not None and kwargs["image_levels"] != '':
        pyramid = BNode()
        no_of_levels = BNode()
        no_of_levels_value = kwargs["image_levels"]
        server_url = kwargs["server_name"]
        file_path = kwargs["path_name"]
        file_path_bnode = BNode()
        if server_url is not None:
            full_file_path = server_url + "/" + file_path
        else:
            full_file_path = file_path

        derivation_event = create_PID_from_triple('pyramid creation for', kwargs["image_file"])
        pyramid_PID = create_PID_from_triple('pyramid of', kwargs["image_file"])
        pyramid_ID = BNode()
        server_PID = generate_placeholder_PID('IIIF Server')

        new_graph.add((getattr(NGO, derivation_event), RDF.type, DIG.D3_Formal_Derivation))
        new_graph.add((getattr(NGO, derivation_event), CRM.P2_has_type, DIG.D3_Formal_Derivation))
        new_graph.add((getattr(NGO, derivation_event), DIG.L21_used_as_derivation_source, getattr(NGO, file_PID)))
        new_graph.add((getattr(NGO, derivation_event), DIG.L22_created_derivative, getattr(NGO, pyramid_PID)))
        new_graph.add((getattr(NGO, pyramid_PID), CRM.P2_has_type, pyramid))
        new_graph.add((pyramid, CRM.P2_has_type, getattr(WD, 'Q3411251')))
        new_graph.add((getattr(WD, 'Q3411251'), RDFS.label, Literal('pyramid', lang="en")))
        new_graph.add((pyramid, CRM.P43_has_dimension, no_of_levels))
        new_graph.add((no_of_levels, RDF.type, CRM.E54_Dimension))
        new_graph.add((no_of_levels, CRM.P2_has_type, CRM.E54_Dimension))
        new_graph.add((no_of_levels, CRM.P90_has_value, Literal(no_of_levels_value, datatype=XSD.double)))
        new_graph.add((no_of_levels, RDFS.label, Literal('Number of pyramidal levels')))
        new_graph.add((getattr(NGO, derivation_event), DIG.L23_used_software_or_firmware, getattr(NGO, server_PID)))
        new_graph.add((getattr(NGO, server_PID), RDF.type, DIG.D14_Software))
        new_graph.add((getattr(NGO, server_PID), CRM.P2_has_type, DIG.D14_Software))
        new_graph.add((getattr(NGO, server_PID), CRM.P2_has_type, getattr(AAT, '300266043')))
        new_graph.add((getattr(AAT, '300266043'), RDFS.label, Literal('servers (computer)', lang="en")))

        new_graph.add((getattr(NGO, pyramid_PID), CRM.P149_is_identified_by, file_path_bnode))
        new_graph.add((file_path_bnode, RDF.type, CRM.E42_Identifier))
        new_graph.add((file_path_bnode, CRM.P2_has_type, CRM.E42_Identifier))
        new_graph.add((file_path_bnode, CRM.P2_has_type, getattr(WD, 'Q1144928')))
        new_graph.add((file_path_bnode, RDF.value, Literal(full_file_path, lang="en")))
        new_graph.add((getattr(WD, 'Q1144928'), RDFS.label, Literal('filename', lang="en")))
        new_graph.add((getattr(NGO, file_PID), RDF.type, DIG.D1_Digital_Object))
        new_graph.add((getattr(NGO, file_PID), CRM.P2_has_type, DIG.D1_Digital_Object))

    if kwargs["image_tile"] is not None and kwargs["image_tile"] != '':
        tile_PID = create_PID_from_triple('image tile of', kwargs["image_file"])
        derivation_event = create_PID_from_triple('image tile creation for', kwargs["image_file"])
        pyramid_PID = create_PID_from_triple('pyramid of', kwargs["image_file"])

        new_graph.add((getattr(NGO, derivation_event), RDF.type, DIG.D3_Formal_Derivation))
        new_graph.add((getattr(NGO, derivation_event), CRM.P2_has_type, DIG.D3_Formal_Derivation))
        new_graph.add((getattr(NGO, derivation_event), DIG.L21_used_as_derivation_source, getattr(NGO, pyramid_PID)))
        new_graph.add((getattr(NGO, derivation_event), DIG.L22_created_derivative, getattr(NGO, tile_PID)))
        new_graph.add((getattr(NGO, tile_PID), RDF.type, CRM.E24_Physical_Human_Made_Thing))
        new_graph.add((getattr(NGO, tile_PID), CRM.P2_has_type, CRM.E24_Physical_Human_Made_Thing))
        new_graph.add((getattr(NGO, tile_PID), CRM.P2_has_type, getattr(WD, 'Q28205699')))
        new_graph.add((getattr(WD, 'Q28205699'), RDFS.label, Literal('Aperio SVS', lang="en")))
        new_graph.add((getattr(NGO, tile_PID), DIG.L56_has_pixel_width, Literal(kwargs["image_tile"], lang="en")))
        new_graph.add((getattr(NGO, tile_PID), DIG.L57_has_pixel_height, Literal(kwargs["image_tile"], lang="en")))

    if kwargs["image_public"] == 'yes':
        new_graph.add((getattr(NGO, kwargs["image_PID"]), CRM.P2_has_type, getattr(AAT, '300055611')))
        new_graph.add((getattr(AAT, '300055611'), RDFS.label, Literal('public domain', lang="en")))
    elif kwargs["image_public"] == 'no':
        new_graph.add((getattr(NGO, kwargs["image_PID"]), CRM.P2_has_type, getattr(AAT, '300055597')))
        new_graph.add((getattr(AAT, '300055597'), RDFS.label, Literal('intellectual property', lang="en")))

    if kwargs["image_caption"] is not None and kwargs["image_caption"] != '':
        new_graph.add((getattr(NGO, kwargs["image_PID"]), CRM.P70_is_documented_in, doc_BN))
        new_graph.add((doc_BN, RDF.type, CRM.E31_Document))
        new_graph.add((doc_BN, CRM.P2_has_type, CRM.E31_Document))
        new_graph.add((doc_BN, CRM.P2_has_type, getattr(WD, 'Q18585177')))
        new_graph.add((getattr(WD, 'Q18585177'), RDFS.label, Literal('caption', lang="en")))
        new_graph.add((doc_BN, RDFS.comment, Literal(kwargs["image_caption"], lang="en")))

    if kwargs["image_type"] == 'thumbnail':
        wd_id = 'Q873806'
        wd_name = 'thumbnail'
    elif kwargs["image_type"] == 'full' or kwargs["image_type"] == 'Whole':
        wd_id = 'Q1250322'
        wd_name = 'digital image'
    elif kwargs["image_type"] == 'Detail':
        wd_id = 'Q56315925'
        wd_name = 'detail'
    elif kwargs["image_type"] == 'artist image':
        wd_id = 'Q134307'
        wd_name = 'portrait'
    else:
        wd_id = None

    if wd_id is not None:
        new_graph.add((getattr(NGO, kwargs["image_PID"]), CRM.P2_has_type, getattr(WD, wd_id)))
        new_graph.add((getattr(WD, wd_id), RDFS.label, Literal(wd_name, lang="en")))

    if kwargs["image_purpose"] is not None and kwargs["image_purpose"] != '':
        if kwargs["image_purpose"] == 'whole painting image':
            wd_id = 'Q3305213'
            wd_name = 'painting'
        elif kwargs["image_purpose"] == 'painting thumbnail':
            wd_id = 'Q873806'
            wd_name = 'thumbnail'
        elif kwargs["image_purpose"] == 'preparation visible on surface':
            wd_id = 'Q11827048'
            wd_name = 'preparation'
        elif kwargs["image_purpose"] == 'colour of ground':
            wd_id = 'P462'
            wd_name = 'color'
        else:
            wd_id is None
            wd_name is None

        image_purpose = BNode()

        new_graph.add((getattr(NGO, kwargs["image_PID"]), CRM.P101_had_as_general_use, image_purpose))
        new_graph.add((image_purpose, RDF.type, CRM.E55_Type))
        new_graph.add((image_purpose, CRM.P2_has_type, CRM.E55_Type))
        new_graph.add((image_purpose, RDF.value, Literal(kwargs["image_purpose"], lang="en")))   
        if wd_id is not None:
            new_graph.add((image_purpose, CRM.P2_has_type, getattr(WD, wd_id)))
            new_graph.add((getattr(WD, wd_id), RDFS.label, Literal(wd_name, lang="en")))
    
    if kwargs["image_photoreference"] is not None and kwargs["image_photoreference"] != '':
        photoreference = BNode()

        new_graph.add((getattr(NGO, kwargs["image_PID"]), CRM.P67_refers_to, photoreference))
        new_graph.add((photoreference, RDFS.label, kwargs["image_photoreference"]))

    if kwargs["image_comment"] is not None and kwargs["image_comment"] != '':
        new_graph.add((getattr(NGO, kwargs["image_PID"]), RDFS.comment, Literal(kwargs["image_comment"], lang="en")))

    if kwargs["image_license"] is not None and kwargs["image_license"] != '':
        image_license = BNode()
        institution_PID = generate_placeholder_PID(kwargs["image_copyright_holder"])

        new_graph.add((getattr(NGO, kwargs["image_PID"]), CRM.P104_is_subject_to, image_license))
        new_graph.add((image_license, RDF.type, CRM.E30_Right))
        new_graph.add((image_license, CRM.P2_has_type, CRM.E30_Right))
        new_graph.add((image_license, RDFS.label, Literal(kwargs["image_license"], lang="en")))
        new_graph.add((getattr(NGO, kwargs["image_copyright_holder"]), CRM.P75_possesses, image_license))

    if kwargs["object_inventory_number"] is not None and kwargs["object_inventory_number"] != '':
        work_image_PID = generate_placeholder_PID(kwargs["object_inventory_number"])
        visual_item_PID = create_PID_from_triple('visual item', kwargs["object_inventory_number"])
        new_graph.add((getattr(NGO, kwargs["image_PID"]), CRM.P65_shows_visual_item, getattr(NGO, visual_item_PID)))
        new_graph.add((getattr(NGO, kwargs["image_PID"]), CRM.P62_depicts, getattr(NGO, work_image_PID)))

    if kwargs["image_classification"] is not None and kwargs["image_classification"] != '':
        if kwargs["image_classification"] == 'Picture':
            aat_ref = '300033618'
            aat_title = 'paintings (visual works)'
        elif kwargs["image_classification"] == 'Picture in Frame':
            aat_ref = '300033618'
            aat_title = 'paintings (visual works)'
        elif kwargs["image_classification"] == 'Sample':
            aat_ref = '300028875'
            aat_title = 'samples'
        else:
            aat_ref is None
            aat_title is None
        
        if aat_ref is not None:
            new_graph.add((getattr(NGO, kwargs["image_PID"]), CRM.P2_has_type, getattr(AAT, aat_ref)))
            new_graph.add((getattr(AAT, aat_ref), RDFS.label, Literal(aat_title, lang="en")))

    return new_graph

def create_model_file_triples(new_graph, **kwargs):

    file_PID = kwargs["model_PID"]
    related_work = kwargs["object_PID"]
    doc_BN = BNode()
    creation_event_PID = create_PID_from_triple('creation', kwargs["mesh_name"])

    new_graph.add((getattr(NGO, file_PID), RDF.type, CRM.E24_Physical_Human_Made_Thing))
    new_graph.add((getattr(NGO, file_PID), CRM.P2_has_type, CRM.E24_Physical_Human_Made_Thing))
    new_graph.add((getattr(NGO, file_PID), CRM.P108i_was_produced_by, getattr(NGO, creation_event_PID)))
    new_graph.add((getattr(NGO, creation_event_PID), RDF.type, CRM.E12_Production))
    new_graph.add((getattr(NGO, creation_event_PID), CRM.P2_has_type, CRM.E12_Production))
    new_graph.add((getattr(NGO, creation_event_PID), CRM.P2_has_type, getattr(AAT, '300404387')))
    new_graph.add((getattr(AAT, '300404387'), RDFS.label, Literal('creating (artistic activity)', lang="en")))

    if kwargs["model_filedate"] is not None and kwargs["model_filedate"] != '':
        image_filedate = BNode()
        image_timespan = create_PID_from_triple('timespan', creation_event_PID)

        new_graph.add((getattr(NGO, creation_event_PID), CRM.P4_has_time_span, getattr(NGO, image_timespan)))
        new_graph.add((getattr(NGO, image_timespan), RDF.type, CRM.E52_Time_span))
        new_graph.add((getattr(NGO, image_timespan), CRM.P2_has_type, CRM.E52_Time_span))
        new_graph.add((getattr(NGO, image_timespan), CRM.P82_at_some_time_within, image_filedate))
        new_graph.add((image_filedate, RDF.type, CRM.E61_Time_Primitive))
        new_graph.add((image_filedate, CRM.P2_has_type, CRM.E61_Time_Primitive))
        new_graph.add((image_filedate, RDFS.label(Literal(kwargs["model_filedate"]))))

    new_graph.add((getattr(NGO, file_PID), RDF.type, CRM.E73_Information_Object))
    new_graph.add((getattr(NGO, file_PID), CRM.P2_has_type, CRM.E73_Information_Object))
    new_graph.add((getattr(NGO, file_PID), RDFS.label, Literal(kwargs["mesh_name"], lang="en")))

    if kwargs["mesh_name"] is not None and kwargs["mesh_name"] != '':
        file_name = BNode()
        new_graph.add((getattr(NGO, file_PID), CRM.P149_is_identified_by, file_name))
        new_graph.add((file_name, RDF.type, CRM.E42_Identifier))
        new_graph.add((file_name, CRM.P2_has_type, CRM.E42_Identifier))
        new_graph.add((file_name, CRM.P2_has_type, getattr(WD, 'Q1144928')))
        new_graph.add((getattr(WD, 'Q1144928'), RDFS.label, Literal('filename', lang="en")))
        new_graph.add((file_name, RDFS.label, Literal(kwargs["mesh_name"], lang="en")))
        new_graph.add((getattr(NGO, file_PID), RDF.type, DIG.D1_Digital_Object))
        new_graph.add((getattr(NGO, file_PID), CRM.P2_has_type, DIG.D1_Digital_Object))

    if kwargs["model_caption"] is not None and kwargs["model_caption"] != '':
        new_graph.add((getattr(NGO, kwargs["model_PID"]), CRM.P70_is_documented_in, doc_BN))
        new_graph.add((doc_BN, RDF.type, CRM.E31_Document))
        new_graph.add((doc_BN, CRM.P2_has_type, CRM.E31_Document))
        new_graph.add((doc_BN, CRM.P2_has_type, getattr(WD, 'Q18585177')))
        new_graph.add((getattr(WD, 'Q18585177'), RDFS.label, Literal('caption', lang="en")))
        new_graph.add((doc_BN, RDFS.comment, Literal(kwargs["model_caption"], lang="en")))

    if kwargs["model_comment"] is not None and kwargs["model_comment"] != '':
        new_graph.add((getattr(NGO, kwargs["model_PID"]), RDFS.comment, Literal(kwargs["model_comment"], lang="en")))

    if kwargs["model_license"] is not None and kwargs["model_license"] != '':
        image_license = BNode()
        institution_PID = generate_placeholder_PID(kwargs["model_copyright"])

        new_graph.add((getattr(NGO, kwargs["model_PID"]), CRM.P104_is_subject_to, image_license))
        new_graph.add((image_license, RDF.type, CRM.E30_Right))
        new_graph.add((image_license, CRM.P2_has_type, CRM.E30_Right))
        new_graph.add((image_license, RDFS.label, Literal(kwargs["model_license"], lang="en")))
        new_graph.add((getattr(NGO, kwargs["model_copyright"]), CRM.P75_possesses, image_license))

    if kwargs["object_inventory_number"] is not None and kwargs["object_inventory_number"] != '':
        work_image_PID = generate_placeholder_PID(kwargs["object_inventory_number"])
        visual_item_PID = create_PID_from_triple('visual item', kwargs["object_inventory_number"])
        new_graph.add((getattr(NGO, kwargs["model_PID"]), CRM.P65_shows_visual_item, getattr(NGO, visual_item_PID)))
        new_graph.add((getattr(NGO, kwargs["model_PID"]), CRM.P62_depicts, getattr(NGO, work_image_PID)))

    return new_graph

def create_classification_triples(new_graph, **kwargs):

    thing_PID = kwargs["thing_PID"]
    classification_event = create_PID_from_triple('classification', kwargs["thing_name"])
    classification_type = BNode()
    classification_name = BNode()

    new_graph.add((getattr(NGO, classification_event), RDF.type, CRM.E17_Type_Assignment))
    new_graph.add((getattr(NGO, classification_event), CRM.P2_has_type, CRM.E17_Type_Assignment))
    new_graph.add((getattr(NGO, classification_event), RDFS.label, Literal('Classification of ' + kwargs["thing_name"], lang="en")))
    new_graph.add((getattr(NGO, classification_event), CRM.P41_classified, getattr(NGO, thing_PID)))
    new_graph.add((getattr(NGO, classification_event), CRM.P2_has_type, classification_type))
    new_graph.add((classification_type, RDF.type, CRM.E55_Type))
    new_graph.add((classification_type, CRM.P2_has_type, CRM.E55_Type))
    new_graph.add((classification_type, RDFS.label, Literal(kwargs["classification_type"], lang="en")))
    new_graph.add((getattr(NGO, classification_event), CRM.P42_assigned, classification_name))
    new_graph.add((classification_name, CRM.P2_has_type, CRM.E55_Type))
    new_graph.add((classification_name, RDF.type, CRM.E55_Type))
    new_graph.add((classification_name, RDFS.label, Literal(kwargs["classification_name"], lang="en")))
    
    if kwargs["aat_link"] is not None and kwargs["aat_link"] != '':
        new_graph.add((getattr(NGO, thing_PID), CRM.P2_has_type, getattr(AAT, kwargs["aat_link"])))

    if kwargs["classification_comment"] is not None and kwargs["classification_comment"] != '':
        new_graph.add((getattr(NGO, thing_PID), RDFS.comment, Literal(kwargs["classification_comment"], lang="en")))

    return new_graph