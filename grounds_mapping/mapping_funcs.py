import pandas as pd
import mysql.connector
from rdflib import Graph, Namespace, Literal, BNode
from rdflib.namespace import OWL, RDF, RDFS, NamespaceManager, XSD
from pdb import set_trace as st
import numpy as np
from common_functions import generate_placeholder_PID, triples_to_csv, triples_to_tsv, create_PID_from_triple, find_aat_value, run_ruby_program, wikidata_query, create_year_dates, check_aat_values, process_name_prefixes
from create_triples import create_object_triples, create_title_triples, create_event_triples, create_dimension_triples, create_medium_triples, create_object_part_triples, create_timespan_triples, create_person_triples, create_role_triples, create_influence_triples, create_institution_triples, create_place_triples, create_extra_timespan_triples, create_reference_triples, create_sample_triples, create_sample_layer_triples, create_sample_component_triples, create_colour_triples, create_preparation_triples, create_protocol_triples, create_measurement_triples, create_location_triples, create_image_file_triples, create_model_file_triples, create_classification_triples

RRO = Namespace("https://rdf.ng-london.org.uk/raphael/ontology/")
RRI = Namespace("https://rdf.ng-london.org.uk/raphael/resource/")
CRM = Namespace("http://www.cidoc-crm.org/cidoc-crm/")
NGO = Namespace("https://data.ng-london.org.uk/")
AAT = Namespace("http://vocab.getty.edu/page/aat/")
TGN = Namespace("http://vocab.getty.edu/page/tgn/")
WD = Namespace("http://www.wikidata.org/entity/")
SCI = Namespace("http://www.cidoc-crm.org/crmsci/")
DIG = Namespace("http://www.cidoc-crm.org/crmdig/")

def map_object(new_graph, **kwargs):
    
    dimension_columns = kwargs["object_part_title_table"].loc[:, "object_height":"object_diameter"]
    
    for _, row in kwargs["object_part_title_table"].iterrows():

        object_PID = generate_placeholder_PID(row["object_inventory_number"])
        assessment_event_PID = create_PID_from_triple('condition assessment', row["object_inventory_number"])
        condition_PID = create_PID_from_triple('condition', row["object_inventory_number"])
        title_PID = BNode()
        short_title_PID = BNode()
        part_PID = create_PID_from_triple(row["object_part_name"], row["object_inventory_number"])

        new_graph = create_object_triples(new_graph=new_graph, object_PID=object_PID, assessment_event_PID=assessment_event_PID, condition_PID=condition_PID, obj=row["object_inventory_number"], comment=row["object_comment"])
        new_graph = create_title_triples(new_graph=new_graph, object_PID=object_PID, title_PID=title_PID, short_title_PID=short_title_PID, long_title=row["title_name"], short_title=row["title_short"], title_comment=row["title_comment"])
        new_graph = create_object_part_triples(new_graph=new_graph, object_PID=object_PID, part_PID=part_PID, part_label=row["object_part_name"], comment=row["object_part_comment"], object_part_type=row["object_part_type"])

        for col in dimension_columns:
            dimension_PID = BNode()
            aat_title, aat_value, aat_unit_title, aat_unit_value = check_aat_values(col)

            create_dimension_triples(new_graph=new_graph, object_PID=object_PID, dimension_PID=dimension_PID, obj=row[col], aat_unit_value=aat_unit_value, aat_unit_title=aat_unit_title, aat_dimension_value=aat_value, aat_dimension_title=aat_title)
        
    for _, row in kwargs["object_medium_support"].iterrows():
    
        object_PID = generate_placeholder_PID(row["object_inventory_number"])
        medium_PID = create_PID_from_triple('medium', row["object_inventory_number"])
        support_PID = create_PID_from_triple('support material', row["object_inventory_number"])
        
        try: 
            medium_aat_number, medium_aat_type = find_aat_value(row["medium_name"], 'medium')
            medium_aat_number = [medium_aat_number]
            medium_aat_type = [medium_aat_type]
        except: 
            medium_aat_number, medium_aat_type = (('300033799', '300389785'), ('oil', 'tempera'))

        try: 
            support_aat_number, support_aat_type = find_aat_value(row["support_name"], 'support')
            support_aat_number = [support_aat_number]
            support_aat_type = [support_aat_type]
        except: 
            support_aat_number is None
            support_aat_type is None

        new_graph = create_medium_triples(new_graph=new_graph, object_PID=object_PID, medium_PID=medium_PID, medium_name=row["medium_name"], aat_number=medium_aat_number, aat_type=medium_aat_type, comment=row["medium_comment"], material_type='medium')
        new_graph = create_medium_triples(new_graph=new_graph, object_PID=object_PID, medium_PID=support_PID, medium_name=row["support_name"], aat_number=support_aat_number, aat_type=support_aat_type, material_type='support', comment=row["support_comment"])
    
    for _, row in kwargs["obj_reference_timespan"].iterrows():
        object_PID = generate_placeholder_PID(row["object_inventory_number"])
        ref_string = 'reference_document_' + str(row["reference_id"])
        reference_PID = generate_placeholder_PID(ref_string)
        timespan_PID = generate_placeholder_PID(row["timespan_name"])
        event_PID = create_PID_from_triple(ref_string, 'creation')
        event_name = 'Creation of reference number ' + str(row["reference_id"])
        
        new_graph = create_reference_triples(new_graph=new_graph, object_PID=object_PID, sample_PID=None, reference_PID=reference_PID, thing_type=row["thing_type"], reference_title=row["reference_title"], reference_id=row["reference_id"], reference_comment=row["reference_comment"], reference_type=row["reference_type"], reference_link=row["reference_link"])
        new_graph = create_timespan_triples(new_graph=new_graph, timespan_PID=timespan_PID, event_PID=event_PID, timespan_start=row["timespan_start"], timespan_end=row["timespan_end"], event_name=event_name, timespan_descriptor=row["timespan_descriptor"], timespan_confidence=None, timespan_name=row["timespan_name"], timespan_comment=row["timespan_comment"])

    for _, row in kwargs["preparation_colours"].iterrows():
        object_PID = generate_placeholder_PID(row["object_inventory_number"])
        prep_name = 'preparation_of_' + row["object_inventory_number"]
        prep_PID = generate_placeholder_PID(prep_name)
        prep_layer_name = 'preparation_layer_of_' + row["object_inventory_number"]
        prep_layer_PID = generate_placeholder_PID(prep_layer_name)

        main_colour_PID = generate_placeholder_PID(row["colour_main_name"])
        modifier_colour_PID = generate_placeholder_PID(row["colour_modifier_name"])
        descriptor_colour_PID = generate_placeholder_PID(row["colour_descriptor_name"])

        main_colour_aat_title = check_aat_values(row["colour_main_name"])[0]
        main_colour_aat_value = check_aat_values(row["colour_main_name"])[1]
        modifier_colour_aat_title = check_aat_values(row["colour_modifier_name"])[0]
        modifier_colour_aat_value = check_aat_values(row["colour_modifier_name"])[1]
        descriptor_colour_aat_title = check_aat_values(row["colour_descriptor_name"])[0]
        descriptor_colour_aat_value = check_aat_values(row["colour_descriptor_name"])[1]

        new_graph = create_preparation_triples(new_graph=new_graph, object_PID=object_PID, prep_layer_PID=prep_layer_PID, prep_layer_name=prep_layer_name, prep_name=prep_name, prep_PID=prep_PID, application_technique=row["application_technique"], preparation_comment=row["preparation_comment"])
        new_graph = create_colour_triples(new_graph=new_graph, object_PID=prep_layer_PID, main_colour_PID=main_colour_PID, modifier_colour_PID=modifier_colour_PID, descriptor_colour_PID=descriptor_colour_PID, main_colour_comment=row["colour_main_comment"], main_colour_name=row["colour_main_name"], modifier_colour_comment=row["colour_modifier_comment"], modifier_colour_name=row["colour_modifier_name"], descriptor_colour_name=row["colour_descriptor_name"], descriptor_colour_comment=row["colour_descriptor_comment"], main_colour_aat_title=main_colour_aat_title, main_colour_aat_value=main_colour_aat_value, modifier_colour_aat_title=modifier_colour_aat_title, modifier_colour_aat_value=modifier_colour_aat_value, descriptor_colour_aat_value=descriptor_colour_aat_value, descriptor_colour_aat_title=descriptor_colour_aat_title)
    
    return new_graph

def map_event(new_graph, **kwargs):
    
    #N.B. add more categories for sample here when the time comes
    
    for _, row in kwargs["object_event_influence"].iterrows():
        
        event_PID = generate_placeholder_PID(row["event_name"])
        object_PID = generate_placeholder_PID(row["object_inventory_number"])
        related_painting_history_event = 'History of ' + row["object_inventory_number"]
        related_painting_history_event_PID = create_PID_from_triple('history', row["object_inventory_number"])
        timespan_PID = generate_placeholder_PID(row["timespan_name"])
        
        if row["event_type"] == 'image acquisition':

            event_property = CRM.P24i_changed_ownership_through
            event_type = CRM.E8_Acquisition
            aat_event_id = getattr(AAT, '300157782')
            aat_event_type = Literal('acquisition (collections management)@en')

            new_graph.add((getattr(NGO, event_PID), CRM.P3_has_note, Literal(row["object_creditline"])))

        elif row["event_type"] == 'painting production':

            event_type = CRM.E12_Production
            event_property = CRM.P108i_was_produced_by
            aat_event_id = getattr(AAT, '300404387')
            aat_event_type = Literal('creating (artistic activity)@en')
            
            if row["manifacturing_process"] is not None:

                technique = generate_placeholder_PID(row["manifacturing_process"])
                new_graph.add((getattr(NGO, event_PID), CRM.P33_used_specific_technique, getattr(NGO, technique)))
                new_graph.add((getattr(NGO, technique), CRM.P1i_identifies, Literal(row["manifacturing_process"])))
                new_graph.add((getattr(NGO, technique), RDF.type, CRM.E29_Design_or_Procedure))
            
            if np.isnan(row["influence_id"]) == False:
                influence_PID = generate_placeholder_PID(row["influence_name"])

                new_graph.add((getattr(NGO, event_PID), CRM.P15_was_influenced_by, getattr(NGO, influence_PID)))

        new_graph = create_event_triples(new_graph=new_graph, event_PID=event_PID, object_PID=object_PID, event_type=event_type, event_property=event_property, aat_event_id=aat_event_id, aat_event_type=aat_event_type, related_painting_history_event=related_painting_history_event, related_painting_history_event_PID=related_painting_history_event_PID, parent_PID=None, parent_name=None)
        new_graph = create_timespan_triples(new_graph=new_graph, timespan_PID=timespan_PID, event_PID=event_PID, timespan_start=row["timespan_start"], timespan_end=row["timespan_end"], event_name=row["event_name"], timespan_descriptor=row["timespan_descriptor"], timespan_confidence=row["timespan_confidence"], timespan_name=row["timespan_name"], timespan_comment=row["timespan_comment"])
        
    for _, row in kwargs["person_parent_table"].iterrows():
        
        event_name = row["event_name"].replace("'","")
        person_name = row["person_name"].replace("'", "")
        event_PID = generate_placeholder_PID(event_name)
        object_PID = generate_placeholder_PID(person_name)
        timespan_PID = generate_placeholder_PID(row["timespan_name"])

        if 'birth' in row["event_name"]:

            event_type = CRM.E67_Birth
            event_property = CRM.P98i_was_born
            aat_event_id = getattr(AAT, '300069672')
            aat_event_type = Literal('births@en')
           
            if row["person_parent_id"] is not None and row["person_parent_id"] != '':
                if np.isnan(row["person_parent_id"]) == False:
                    parent_PID = generate_placeholder_PID(row["person_parent_id"])
                    parent_name = row["person_parent_name"]
                    new_graph = create_event_triples(new_graph=new_graph, event_PID=event_PID, object_PID=object_PID, event_type=event_type, event_property=event_property, aat_event_id=aat_event_id, aat_event_type=aat_event_type, related_painting_history_event=None, related_painting_history_event_PID=None, parent_PID=parent_PID, parent_name=parent_name)
                else:
                    new_graph = create_event_triples(new_graph=new_graph, event_PID=event_PID, object_PID=object_PID, event_type=event_type, event_property=event_property, aat_event_id=aat_event_id, aat_event_type=aat_event_type, related_painting_history_event=None, related_painting_history_event_PID=None, parent_PID=None, parent_name=None)

            new_graph = create_timespan_triples(new_graph=new_graph, timespan_PID=timespan_PID, event_PID=event_PID, timespan_start=row["timespan_start"], timespan_end=row["timespan_end"], event_name=row["event_name"], timespan_descriptor=row["timespan_descriptor"], timespan_confidence=row["timespan_confidence"], timespan_name=row["timespan_name"], timespan_comment=row["timespan_comment"])

        elif 'death' in row["event_name"]:

            event_type = CRM.E69_Death
            event_property = CRM.P100i_died_in
            aat_event_id = getattr(AAT, '300151836')
            aat_event_type = Literal('deaths@en')

            new_graph = create_event_triples(new_graph=new_graph, event_PID=event_PID, object_PID=object_PID, event_type=event_type, event_property=event_property, aat_event_id=aat_event_id, aat_event_type=aat_event_type, related_painting_history_event=None, related_painting_history_event_PID=None, parent_PID=None, parent_name=None)
            new_graph = create_timespan_triples(new_graph=new_graph, timespan_PID=timespan_PID, event_PID=event_PID, timespan_start=row["timespan_start"], timespan_end=row["timespan_end"], event_name=row["event_name"], timespan_descriptor=row["timespan_descriptor"], timespan_confidence=row["timespan_confidence"], timespan_name=row["timespan_name"], timespan_comment=row["timespan_comment"])
    
    for _, row in kwargs["sample_event"].iterrows():

        event_name = row["event_name"]
        event_PID = generate_placeholder_PID(event_name)
        sample_name = 'Sample_no_' + str(row["sample_id"])
        sample_PID = generate_placeholder_PID(sample_name)

        if row["event_type"] == 'sampling':

            event_type = CRM.E12_Production
            event_property = CRM.P108i_was_produced_by
            aat_event_id = getattr(AAT, '300379429')
            aat_event_type = 'sampling'

            new_graph = create_event_triples(new_graph=new_graph, event_PID=event_PID, object_PID=sample_PID, event_type=event_type, event_property=event_property, aat_event_id=aat_event_id, aat_event_type=aat_event_type, related_painting_history_event=related_painting_history_event, related_painting_history_event_PID=related_painting_history_event_PID, parent_PID=None, parent_name=None)
    
    for _, row in kwargs["event_protocol"].iterrows():

        document_PID = generate_placeholder_PID(row["protocol_name"])
        event_PID = generate_placeholder_PID(row["event_name"])
        timespan_PID = generate_placeholder_PID(row["timespan_name"])
        institution_PID = generate_placeholder_PID(row["institution_name"])
        technique_PID = generate_placeholder_PID(row["technique_name"])

        new_graph = create_protocol_triples(new_graph=new_graph, document_PID=document_PID, institution_PID=institution_PID, event_PID=event_PID, technique_PID=technique_PID, protocol_type=row["protocol_type"], protocol_name=row["protocol_name"], protocol_comment=row["protocol_comment"], protocol_file=row["protocol_file"], technique_name=row["technique_name"], technique_full_name=row["technique_full_name"], technique_comment=row["technique_comment"], technique_link=row["technique_link"])
        new_graph = create_timespan_triples(new_graph=new_graph, timespan_PID=timespan_PID, event_PID=event_PID, timespan_start=row["timespan_start"], timespan_end=row["timespan_end"], event_name=row["event_name"], timespan_descriptor=row["timespan_descriptor"], timespan_confidence=None, timespan_name=row["timespan_name"], timespan_comment=row["timespan_comment"])

    return new_graph

def map_person(new_graph, **kwargs):

    kwargs["person_role_institution"]["full_name"] = process_name_prefixes(kwargs["person_role_institution"])

    for _, row in kwargs["person_role_institution"].iterrows():

        person_name = row["full_name"].replace("'", "")
        person_PID = generate_placeholder_PID(person_name)
        institution_PID = generate_placeholder_PID(row["institution_name"])

        new_graph = create_person_triples(new_graph=new_graph, person_PID=person_PID, person_name=row["full_name"], comment=row["person_comment"], person_other_name=row["person_other_name"], person_contact=row["person_contact"])
        new_graph = create_role_triples(new_graph=new_graph, person_title=row["person_title"], person_PID=person_PID, role_name=row["role_name"], role_comment=row["role_comment"], institution_PID=institution_PID)
        
    for _, row in kwargs["person_influence"].iterrows():

        person_name = row["full_name"].replace("'", "")
        person_PID = generate_placeholder_PID(person_name)
        influence_PID = generate_placeholder_PID(row["influence_name"])
        influenced_event_PID = create_PID_from_triple('production of the works of', person_name)
        influenced_event_name = 'Production of the works of ' + person_name

        new_graph = create_influence_triples(new_graph=new_graph, person_name=person_name, person_PID=person_PID, influence_name=row["influence_name"], influence_PID=influence_PID, influenced_event_PID=influenced_event_PID, influenced_event_name=influenced_event_name, aat_link=row["aat_link"], comment=row["influence_comment"])

    for _, row in kwargs["parent_influence"].iterrows():

        person_name = row["full_name_child"].replace("'", "")
        person_PID = generate_placeholder_PID(person_name)
        influence_PID = generate_placeholder_PID(row["influence_name_parent"])
        influenced_event_PID = create_PID_from_triple('production of the works of', person_name)
        influenced_event_name = 'Production of the works of ' + person_name

        new_graph.add((getattr(influenced_event_PID), CRM.P15_was_influenced_by, getattr(NGO, influence_PID)))

    return new_graph

def map_place(new_graph, **kwargs):
    
    for _, row in kwargs["place_institution_parent"].iterrows():

        institution_PID = generate_placeholder_PID(row["institution_name"])
        building_PID = create_PID_from_triple('building of', row["institution_name"])
        location_PID = generate_placeholder_PID(row["place_name"] + ' (location)')
        parent_location_PID = generate_placeholder_PID(row["parent_place_name"] + ' (location)')

        new_graph = create_institution_triples(new_graph=new_graph, institution_PID=institution_PID, institution_name=row["institution_name"], institution_acronym=row["institution_acronym"], webpage=row["webpage"], institution_type=row["institution_type"], institution_comment=row["institution_comment"])
        new_graph = create_place_triples(new_graph=new_graph, location_PID=location_PID, institution_PID=institution_PID, building_PID=building_PID, place_name=row["place_name"], latitude=row["latitude"], longitude=row["longitude"], place_comment=row["place_comment"], place_type=row["place_type"])
        new_graph.add((getattr(NGO, location_PID), CRM.P89_falls_within, getattr(NGO, parent_location_PID)))

    for _, row in kwargs["place_timespan"].iterrows():

        event_PID = generate_placeholder_PID(row["place_name"] + ' (location)')
        timespan_PID = generate_placeholder_PID(row["timespan_name"])
        event_name = 'Existence of ' + row["place_name"]
        event_PID = generate_placeholder_PID(event_name)

        new_graph = create_timespan_triples(new_graph=new_graph, timespan_PID=timespan_PID, event_PID=event_PID, timespan_start=row["timespan_start"], timespan_end=row["timespan_end"], event_name=event_name, timespan_descriptor=row["timespan_descriptor"], timespan_confidence=None, timespan_name=row["timespan_name"], timespan_comment=row["timespan_comment"])

    for _, row in kwargs["institution_classification"].iterrows():
        thing_PID = generate_placeholder_PID(row["institution_name"])

        new_graph = create_classification_triples(new_graph=new_graph, thing_PID=thing_PID, thing_name=row["institution_name"], classification_name=row["classification_name"], aat_link=row["aat_link"], classification_type=row["classification_type"], classification_comment=row["classification_comment"])

    return new_graph

def map_extra_timespan_info(new_graph, **kwargs):

    for _, row in kwargs["full_timespan"].iterrows():
        timespan_PID = generate_placeholder_PID(row["timespan_name"])

        new_graph = create_extra_timespan_triples(new_graph=new_graph, timespan_PID=timespan_PID, timespan_extra_relation=row["timespan_extra_relation"], timespan_extra_group=row["timespan_extra_group"], timespan_extra_name=row["timespan_extra_name"], timespan_extra_comment=row["timespan_extra_comment"])

    return new_graph

def map_sample(new_graph, **kwargs):

    for _, row in kwargs["sample_timespan_event"].iterrows():

        object_inventory_number = row["object_inventory_number"]
        sample_id = row["sample_id"]
        sample_layer_id = row["sample_layer_id"]

        if row["object_inventory_number"] is None:
            split_event_string = row["event_name"].split()

            object_inventory_number = split_event_string[4]
            sample_id = split_event_string[1]
            sample_layer_id = None

        object_PID = generate_placeholder_PID(object_inventory_number)
        object_part_id = 'Object_part_no_' + str(row["object_part_id"])
        object_part_PID = generate_placeholder_PID(object_part_id)
        sample_id = 'Sample_no_' + str(sample_id)
        sample_PID = generate_placeholder_PID(sample_id)
        layer_id = 'Layer_no_' + str(sample_layer_id)
        layer_PID = generate_placeholder_PID(layer_id)
        component_ID = 'Component_no_' + str(row["sample_component_id"])
        component_PID = generate_placeholder_PID(component_ID)
        
        new_graph = create_sample_triples(new_graph=new_graph, sample_PID=sample_PID, object_part_PID=object_part_PID, object_inventory_number=row["object_inventory_number"], object_part_name=row["object_part_name"], sample_type=row["sample_type"], sample_comment=row["sample_comment"], event_name=row["event_name"], event_type=row["event_type"])
        if sample_layer_id is not None:
            new_graph = create_sample_layer_triples(new_graph=new_graph, event_name=row["event_name"], sample_PID=sample_PID, layer_PID=layer_PID, object_PID=object_PID, object_layer_number=row["object_layer_number"], sample_layer_number=row["sample_layer_number"], sample_layer_comment=row["sample_layer_comment"], sample_number=row["sample_number"], object_inventory_number=row["object_inventory_number"])
        if row["sample_layer_thickness"] is not None and row["sample_layer_thickness"] != '':
            new_graph = create_dimension_triples(new_graph=new_graph, dimension_PID=BNode(), object_PID=layer_PID, obj=row["sample_layer_thickness"], aat_dimension_value='300055646', aat_dimension_title='thickness', aat_unit_value='300198990', aat_unit_title='micrometers')     
        if sample_layer_id is not None:
            new_graph = create_sample_component_triples(new_graph=new_graph, layer_PID=layer_PID, sample_PID=sample_PID, component_PID=component_PID, event_name=row["event_name"], sample_component_size=row["sample_component_size"], sample_component_number=row["sample_component_number"], sample_component_function=row["sample_component_function"], sample_comp_function_confidence=row["sample_comp_function_confidence"], sample_component_comment=row["sample_component_comment"], sample_number=row["sample_number"], object_inventory_number=row["object_inventory_number"], sample_component_amount=row["sample_component_amount"])
        
    for _, row in kwargs["sample_colour"].iterrows():   
        layer_id = 'Layer_no_' + str(row["sample_layer_id"]) 
        object_PID = generate_placeholder_PID(layer_id)

        main_colour_PID = generate_placeholder_PID(row["colour_main_name"])
        modifier_colour_PID = generate_placeholder_PID(row["colour_modifier_name"])
        descriptor_colour_PID = generate_placeholder_PID(row["colour_descriptor_name"])

        main_colour_aat_title = check_aat_values(row["colour_main_name"])[0]
        main_colour_aat_value = check_aat_values(row["colour_main_name"])[1]
        modifier_colour_aat_title = check_aat_values(row["colour_modifier_name"])[0]
        modifier_colour_aat_value = check_aat_values(row["colour_modifier_name"])[1]
        descriptor_colour_aat_title = check_aat_values(row["colour_descriptor_name"])[0]
        descriptor_colour_aat_value = check_aat_values(row["colour_descriptor_name"])[1]

        new_graph = create_colour_triples(new_graph=new_graph, object_PID=object_PID, main_colour_PID=main_colour_PID, modifier_colour_PID=modifier_colour_PID, descriptor_colour_PID=descriptor_colour_PID, main_colour_comment=row["colour_main_comment"], main_colour_name=row["colour_main_name"], modifier_colour_comment=row["colour_modifier_comment"], modifier_colour_name=row["colour_modifier_name"], descriptor_colour_name=row["colour_descriptor_name"], descriptor_colour_comment=row["colour_descriptor_comment"], main_colour_aat_title=main_colour_aat_title, main_colour_aat_value=main_colour_aat_value, modifier_colour_aat_title=modifier_colour_aat_title, modifier_colour_aat_value=modifier_colour_aat_value, descriptor_colour_aat_value=descriptor_colour_aat_value, descriptor_colour_aat_title=descriptor_colour_aat_title)
     
    for _, row in kwargs["sample_component_colours"].iterrows():
        component_ID = 'Component_no_' + str(row["sample_component_id"])
        object_PID = generate_placeholder_PID(component_ID)

        main_colour_PID = generate_placeholder_PID(row["colour_main_name"])
        descriptor_colour_PID = generate_placeholder_PID(row["colour_descriptor_name"])

        main_colour_aat_title = check_aat_values(row["colour_main_name"])[0]
        main_colour_aat_value = check_aat_values(row["colour_main_name"])[1]
        descriptor_colour_aat_title = check_aat_values(row["colour_descriptor_name"])[0]
        descriptor_colour_aat_value = check_aat_values(row["colour_descriptor_name"])[1]

        new_graph = create_colour_triples(new_graph=new_graph, object_PID=object_PID, main_colour_PID=main_colour_PID, modifier_colour_PID=None, descriptor_colour_PID=descriptor_colour_PID, main_colour_comment=row["colour_main_comment"], main_colour_name=row["colour_main_name"], modifier_colour_comment=None, modifier_colour_name=None, descriptor_colour_name=row["colour_descriptor_name"], descriptor_colour_comment=row["colour_descriptor_comment"], main_colour_aat_title=main_colour_aat_title, main_colour_aat_value=main_colour_aat_value, modifier_colour_aat_title=None, modifier_colour_aat_value=None, descriptor_colour_aat_value=descriptor_colour_aat_value, descriptor_colour_aat_title=descriptor_colour_aat_title)
        
    for _, row in kwargs["sample_component_parents"].iterrows():
        component_ID = 'Component_no_' + str(row["sample_component_id"])
        component_PID = generate_placeholder_PID(component_ID)
        parent_component_id = 'Component_no_' + str(row["sample_component_parent_id"])
        parent_component_pid = generate_placeholder_PID(parent_component_id)

        new_graph.add((getattr(NGO, parent_component_pid), CRM.P45_consists_of, getattr(NGO, component_PID)))

    for _, row in kwargs["measurement_materials"].iterrows():
        event_PID = generate_placeholder_PID(row["event_name"])

        new_graph = create_measurement_triples(new_graph=new_graph, event_PID=event_PID, event_name=row["event_name"], object_inventory_number=row["object_inventory_number"], material_name=row["material_name"], material_value=row["material_value"], material_value_percent=row["material_value%"], material_comment=row["material_comment"], material_type=row["material_type"], material_link=row["material_link"], material_class=row["material_class"], material_subclass=row["material_subclass"], measurement_comment=row["measurement_comment"], measurementXcomposition_comment=row["measurementXcomposition_comment"], result_confidence=row["result_confidence"])
    
    for _, row in kwargs["sample_location"].iterrows():
        location_PID = generate_placeholder_PID(row["location_name"])

        new_graph = create_location_triples(new_graph=new_graph, object_part_name=row["object_part_name"], object_inventory_number=row["object_inventory_number"], location_PID=location_PID, location_name=row["location_name"], location_description=row["location_description"], object_side=row["object_side"], location_comment=row["location_comment"], image_location_x=row["image_location_x"], image_location_y=row["image_location_y"], image_location_w=row["image_location_w"], image_location_h=row["image_location_h"], model_location_x=row["3Dmodel_location_x"], model_location_y=row["3Dmodel_location_y"], model_location_z=row["3Dmodel_location_z"], image_file=row["image_file"], mesh_name=row["mesh_name"])
        new_graph = create_dimension_triples(new_graph=new_graph, dimension_PID=BNode(), object_PID=layer_PID, obj=row["sample_layer_thickness"], aat_dimension_value='300055646', aat_dimension_title='thickness', aat_unit_value='300198990', aat_unit_title='micrometers')

    for _, row in kwargs["sample_reference"].iterrows():
        sample_id = 'Sample_no_' + str(row["sample_id"])
        sample_PID = generate_placeholder_PID(sample_id)
        ref_string = 'reference_document_' + str(row["reference_id"])
        reference_PID = generate_placeholder_PID(ref_string)
        
        new_graph = create_reference_triples(new_graph=new_graph, object_PID=None, sample_PID=sample_PID, reference_PID=reference_PID, thing_type=row["thing_type"], reference_title=row["reference_title"], reference_id=row["reference_id"], reference_comment=row["reference_comment"], reference_type=row["reference_type"], reference_link=row["reference_link"])

    return new_graph

def map_image(new_graph, **kwargs):

    for _, row in kwargs["image_path_server_etc"].iterrows():
        image_PID = generate_placeholder_PID(row["image_name"])
        object_PID = generate_placeholder_PID(row["object_inventory_number"])

        new_graph = create_image_file_triples(new_graph=new_graph, image_PID=image_PID, object_PID=object_PID, image_name=row["image_name"], image_file=row["image_file"], image_width=row["image_width"], image_height=row["image_height"], image_format=row["image_format"], image_levels=row["image_levels"], image_ppmm=row["image_ppmm"], image_tile=row["image_tile"], image_public=row["image_public"], image_caption=row["image_caption"], image_type=row["image_type"], image_purpose=row["image_purpose"], image_lightsource=row["image_lightsource"], image_optical_spec=row["image_optical_spec"], image_aspect=row["image_aspect"], image_filedate=row["image_filedate"], image_copyright_holder=row["image_copyright_holder"], image_license=row["image_license"], image_photoreference=row["image_photoreference"], image_classification=row["image_classification"], server_id=row["server_id"], path_id=row["path_id"], image_comment=row["image_comment"], object_inventory_number=row["object_inventory_number"], sample_id=row["sample_id"], person_name=row["person_name"], person_prefix_name=row["person_prefix_name"], path_name=row["path_name"], path_original=row["path_original"], path_public=row["path_public"], server_name=row["server_name"])        

    for _, row in kwargs["model_path_server_etc"].iterrows():
        image_PID = generate_placeholder_PID(row["3d_model_name"])
        object_PID = generate_placeholder_PID(row["object_inventory_number"])

        new_graph = create_model_file_triples(new_graph=new_graph, model_pid=image_PID, object_PID=object_PID, mesh_name=row["mesh_name"], model_filedate=row["3Dmodel_filedate"], model_caption=row["3Dmodel_caption"], model_comment=row["3Dmodel_comment"], object_inventory_number=row["object_inventory_number"], model_license=row["3Dmodel_license"], model_copyright=row["3Dmodel_copyright"])

    return new_graph
