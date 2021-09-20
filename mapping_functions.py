from rdflib import Graph, Namespace, Literal, BNode
from rdflib.namespace import RDF, RDFS, NamespaceManager, XSD
from rdflib.serializer import Serializer
from SPARQLWrapper import SPARQLWrapper, JSON
from create_triples import create_time_span_triples, create_role_triples, create_dimension_triples, create_event_triples, create_institution_triples, create_location_triples, create_medium_triples, create_reference_triples, create_collection_triples, create_title_triples, create_identifier_triples, create_type_triples, create_time_span_triples, create_actor_event_relationship_triples, create_name_triples, create_comment_triples, create_room_triples, create_area_of_room_triples, create_file_triples, create_examination_event_triples, create_triples_from_reference_string, create_modification_event_triples, create_image_production_event_triples, create_provenance_triples, create_sampling_triples
from common_functions import generate_placeholder_PID, wikidata_query, create_PID_from_triple, query_objects, get_property, pretty_print_triples
from pdb import set_trace as st

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

def map_object(new_graph, old_graph):
    for painting_id, _, _ in old_graph.triples((None, RDF.type, getattr(RRO,'RC12.Painting'))):
        for subj, pred, obj in old_graph.triples((painting_id, None, None)):
            subject_PID = generate_placeholder_PID(subj)
            new_graph = create_title_triples(new_graph, subject_PID, subj, pred, obj)
            new_graph = create_medium_triples(new_graph, subject_PID, subj, pred, obj)
            new_graph = create_collection_triples(new_graph, subject_PID, subj, pred, obj)
            new_graph = create_dimension_triples(new_graph, subject_PID, subj, pred, obj)
            new_graph = create_identifier_triples(new_graph, subject_PID, pred, obj)
            new_graph = create_type_triples(new_graph, subject_PID, pred, obj)
            new_graph = create_location_triples(new_graph, subject_PID, subj, pred, obj)

    return new_graph

def map_event(new_graph, old_graph):
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
                new_graph = create_event_triples(new_graph, subject_PID, event_PID, subj, pred)
            for subj, pred, obj in old_graph.triples((event_name, None, None)):
                subject_PID = generate_placeholder_PID(subj)
                new_graph = create_time_span_triples(new_graph, subject_PID, subj, pred, obj)
                new_graph = create_comment_triples(new_graph, subject_PID, pred, obj)
                new_graph = create_actor_event_relationship_triples(new_graph, subject_PID, pred, obj)
                #location

    return new_graph

def map_person(new_graph, old_graph):
    for person_name, _, _ in old_graph.triples((None, RDF.type, getattr(RRO, 'RC40.Person'))):
        for subj, pred, obj in old_graph.triples((person_name, None, None)):
            subject_PID = generate_placeholder_PID(subj)
            new_graph = create_type_triples(new_graph, subject_PID, pred, obj)
            new_graph = create_name_triples(new_graph, subject_PID, subj, pred, obj)
            new_graph = create_role_triples(new_graph, subject_PID, pred, obj)
            new_graph = create_comment_triples(new_graph, subject_PID, pred, obj)
            new_graph = create_location_triples(new_graph, subject_PID, subj, pred, obj)

    return new_graph

def map_institution(new_graph, old_graph):
    for institution_name, _, _ in old_graph.triples((None, RDF.type, getattr(RRO, 'RC41.Institution'))):
        subject_PID = generate_placeholder_PID(institution_name)
        
        wikidata_name = wikidata_query(institution_name, 'institution')
        if wikidata_name is not None:
            new_graph.add((getattr(NGO, subject_PID), OWL.sameAs, getattr(WD, wikidata_name)))
        
        new_graph = create_institution_triples(old_graph, new_graph, subject_PID, institution_name)
    
    for room_name, _, _ in old_graph.triples((getattr(RRI, 'Room_8'), RDF.type, getattr(RRO, 'RC11.Room'))):
        subject_PID = generate_placeholder_PID(room_name)

        for subj, pred, obj in old_graph.triples((room_name, None, None)):
            new_graph = create_room_triples(new_graph, subject_PID, subj, pred, obj)

    for area_name, _, _ in old_graph.triples((None, RDF.type, getattr(RRO, 'RC264.Area_in_Room'))):
        subject_PID = generate_placeholder_PID(area_name)

        for subj, pred, obj in old_graph.triples((area_name, None, None)):
            new_graph = create_area_of_room_triples(new_graph, subject_PID, subj, pred, obj)
    
    return new_graph

def map_document(new_graph, old_graph):
    
    for doc_name, _, _ in old_graph.triples((None, RDF.type, getattr(RRO, 'RC26.Digital_Document'))):
        subject_PID = generate_placeholder_PID(doc_name)
        database_PID = generate_placeholder_PID('The National Gallery Collection Image Database')
        text_PID = create_PID_from_triple('text', doc_name)

        new_graph.add((getattr(NGO, subject_PID), RDF.type, DIG.D1_Digital_Object))
        new_graph.add((getattr(NGO, subject_PID), CRM.P2_has_type, DIG.D1_Digital_Object))
        new_graph.add((getattr(NGO, subject_PID), CRM.P70i_is_documented_in, getattr(NGO, database_PID)))
        new_graph.add((getattr(NGO, database_PID), RDFS.label, Literal('The National Gallery Collection Image Database', lang="en")))
        new_graph.add((getattr(NGO, subject_PID), CRM.P165_incorporates, getattr(NGO, text_PID)))
        new_graph.add((getattr(NGO, text_PID), RDF.type, CRM.E90_Symbolic_Object))
        new_graph.add((getattr(NGO, text_PID), CRM.P2_has_type, CRM.E90_Symbolic_Object))

        for subj, pred, obj in old_graph.triples((doc_name, None, None)):
            new_graph = create_type_triples(new_graph, subject_PID, pred, obj)
            new_graph = create_reference_triples(new_graph, subject_PID, subj, pred, obj)
            new_graph = create_file_triples(new_graph, old_graph, subject_PID, subj, pred, obj)
            new_graph = create_examination_event_triples(new_graph, old_graph, subject_PID, subj, pred, obj, doc_type='document')
            new_graph = create_actor_event_relationship_triples(new_graph, subject_PID, pred, obj)
    
    for doc_name, _, _ in old_graph.triples((None, RDF.type, getattr(RRO, 'RC220.Digital_Text'))):
        subject_PID = generate_placeholder_PID(doc_name)
        database_PID = generate_placeholder_PID('The Raphael Research Resource')

        new_graph.add((getattr(NGO, subject_PID), CRM.P70i_is_documented_in, getattr(NGO, database_PID)))
        new_graph.add((getattr(NGO, database_PID), RDFS.label, Literal('The Raphael Research Resource', lang="en")))

        for subj, pred, obj in old_graph.triples((doc_name, None, None)):    
            new_graph = create_type_triples(new_graph, subject_PID, pred, obj)
            new_graph = create_reference_triples(new_graph, subject_PID, subj, pred, obj)
            new_graph = create_file_triples(new_graph, old_graph, subject_PID, subj, pred, obj)
            new_graph = create_examination_event_triples(new_graph, old_graph, subject_PID, subj, pred, obj, doc_type='document')
            new_graph = create_actor_event_relationship_triples(new_graph, subject_PID, pred, obj)
            new_graph = create_comment_triples(new_graph, subject_PID, pred, obj)

            if pred == getattr(RRO, 'RP98.is_in_project_category') and (obj == getattr(RRI, 'RCL184.General_Bibliography') or obj == getattr(RRI, 'RCL185.Exhibition_and_Loan_History')):
                related_bibliography = query_objects(old_graph, subj, getattr(RRO, 'RP237.has_content'), None)
                painting_id = query_objects(old_graph, subj, getattr(RRO, 'RP40.is_related_to'), None)[0]
                for _,_,o in old_graph.triples((subj, getattr(RRO, 'RP237.has_content'), None)):
                    references_list = o.split('\n')
                    create_triples_from_reference_string(new_graph, references_list, painting_id)
    
    return new_graph

def map_image(new_graph, old_graph):
    for image_name, _, _ in old_graph.triples((None, RDF.type, getattr(RRO, 'RC25.Image'))):
        st()
        subject_PID = generate_placeholder_PID(image_name)
        related_works = query_objects(old_graph, image_name, getattr(RRO, 'RP40.is_related_to'), None)
        database_PID = generate_placeholder_PID('The National Gallery Collection Image Database')

        new_graph.add((getattr(NGO, subject_PID), RDF.type, DIG.D1_Digital_Object))
        new_graph.add((getattr(NGO, subject_PID), CRM.P2_has_type, DIG.D1_Digital_Object))
        new_graph.add((getattr(NGO, subject_PID), CRM.P70i_is_documented_in, getattr(NGO, database_PID)))
        new_graph.add((getattr(NGO, database_PID), RDFS.label, Literal('The National Gallery Collection Image Database', lang="en")))

        if (image_name, getattr(RRO, 'RP98.is_in_project_category'), getattr(RRI, 'RCL183.Provenance')) in old_graph:
            doc_type = 'document'
        else:
            doc_type = 'image'

        #only for images that are actually pictures of paintings; otherwise they must be treated as documents - how do we define this?????
        for work in related_works:
            if (getattr(RRI, work), RDF.type, getattr(RRO, 'RC12.Painting')) in old_graph:
                painting_PID = generate_placeholder_PID(work)
                image_PID = create_PID_from_triple('visual item', work)

                new_graph.add((getattr(NGO, subject_PID), CRM.P65_shows_visual_item, getattr(NGO, image_PID)))
                new_graph.add((getattr(NGO, image_PID), RDF.type, CRM.E36_Visual_Item))
                new_graph.add((getattr(NGO, image_PID), CRM.P2_has_type, CRM.E36_Visual_Item))
                new_graph.add((getattr(NGO, image_PID), CRM.P1i_is_identified_by, Literal("Visual image of " + work, lang="en")))
                new_graph.add((getattr(NGO, image_PID), CRM.P138_represents, getattr(NGO, painting_PID)))
                new_graph.add((getattr(NGO, subject_PID), CRM.P62_depicts, getattr(NGO, painting_PID)))

        for subj, pred, obj in old_graph.triples((image_name, None, None)):
            new_graph = create_file_triples(new_graph, old_graph, subject_PID, subj, pred, obj)
            st()
            new_graph = create_dimension_triples(new_graph, subject_PID, subj, pred, obj)
            new_graph = create_type_triples(new_graph, subject_PID, pred, obj)
            new_graph = create_modification_event_triples(new_graph, old_graph, subject_PID, subj, pred, obj)
            new_graph = create_examination_event_triples(new_graph, old_graph, subject_PID, subj, pred, obj, doc_type=doc_type)
            new_graph = create_image_production_event_triples(new_graph, old_graph, subject_PID, subj, pred, obj)
            new_graph = create_provenance_triples(new_graph, old_graph, subject_PID, subj, pred, obj)
            new_graph = create_reference_triples(new_graph, subject_PID, subj, pred, obj)
    st()
    return new_graph

def map_sample(new_graph, old_graph):
    for sample_name, _, _ in old_graph.triples((None, RDF.type, getattr(RRO, 'RC23.Sample'))):

        subject_PID = generate_placeholder_PID(sample_name)

        for subj, pred, obj in old_graph.triples((sample_name, None, None)):
            new_graph = create_sampling_triples(new_graph, old_graph, subject_PID, subj, pred, obj)
            new_graph = create_examination_event_triples(new_graph, old_graph, subject_PID, subj, pred, obj, doc_type='image')

    return new_graph

def map_leftover_categories(new_graph, old_graph):
    
    for file_path, _, _ in old_graph.triples((None, RDF.type, getattr(RRO, 'RC223.Computer_Path'))):
        file_path_bn = BNode()
        file_path = get_property(file_path, keep_underscores=True)
        new_graph.add((file_path_bn, RDF.type, CRM.E73_Information_Object))
        new_graph.add((file_path_bn, CRM.P2_has_type, CRM.E73_Information_Object))
        new_graph.add((file_path_bn, RDFS.label, Literal(file_path)))
    for file_path, _, _ in old_graph.triples((None, RDF.type, getattr(RRO, 'RC280.IIPImage_Server'))):
        file_path_bn = BNode()
        file_path = get_property(file_path, keep_underscores=True)
        new_graph.add((file_path_bn, RDF.type, CRM.E73_Information_Object))
        new_graph.add((file_path_bn, CRM.P2_has_type, CRM.E73_Information_Object))
        new_graph.add((file_path_bn, RDFS.label, Literal(file_path)))
    for file_path, _, _ in old_graph.triples((None, RDF.type, getattr(RRO, 'RC287.Commercial_Link'))):
        file_path_bn = BNode()
        file_path = get_property(file_path, keep_underscores=True)
        new_graph.add((file_path_bn, RDF.type, CRM.E73_Information_Object))
        new_graph.add((file_path_bn, CRM.P2_has_type, CRM.E73_Information_Object))
        new_graph.add((file_path_bn, RDFS.label, Literal(file_path)))
    
    for boolean_obj, _, _ in old_graph.triples((None, RDF.type, getattr(RRO, 'RC227.Boolean'))):
        if boolean_obj == getattr(RRI, 'RCL228.Yes'):
            boolean_literal = 'Yes'
        elif boolean_obj == getattr(RRI, 'RCL229.No'):
            boolean_literal = 'No'

        new_graph.add((Literal(boolean_literal), RDF.type, CRM.E59_Primitive_Value))
        new_graph.add((Literal(boolean_literal), CRM.P2_has_type, CRM.E59_Primitive_Value))
    
    for language, _, _ in old_graph.triples((None, RDF.type, getattr(RRO, 'RC232.Language'))):
        language_bn = BNode()
        for subj, pred, obj in old_graph.triples((language, None, None)): 
            if pred == RDF.type:
                new_graph.add((language_bn, RDF.type, CRM.E56_Language))
                new_graph.add((language_bn, CRM.P2_has_type, CRM.E56_Language))
                new_graph.add((language_bn, RDFS.label, Literal(get_property(subj), lang="en")))
            elif pred == getattr(RRO, 'RP56.has_name'):
                wd_result = wikidata_query(obj, 'language')
                new_graph.add((language_bn, CRM.P72_has_language, Literal(obj, lang="en")))
                if wd_result is not None:
                    new_graph.add((language_bn, CRM.P72_has_language, getattr(WD, wd_result)))
    
    return new_graph