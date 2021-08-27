from rdflib import Graph, Namespace
import sys

sys.path.append("C:\\Users\\orla.delaney\\Documents\\Code\\OWL-RL")
import owlrl 

RRO = Namespace("https://rdf.ng-london.org.uk/raphael/ontology/")
RRI = Namespace("https://rdf.ng-london.org.uk/raphael/resource/")
CRM = Namespace("http://www.cidoc-crm.org/cidoc-crm/")
NGO = Namespace("https://round4.ng-london.org.uk/ex/lcd/")
AAT = Namespace("http://vocab.getty.edu/page/aat/")
TGN = Namespace("http://vocab.getty.edu/page/tgn/")
WD = Namespace("http://www.wikidata.org/entity/")
DIG = Namespace("http://www.cidoc-crm.org/crmdig/")
SCI = Namespace("http://www.cidoc-crm.org/crmsci/")

g = Graph()
g.parse("outputs/raphael_final.xml", format="xml")
g.namespace_manager.bind('crm',CRM)
g.namespace_manager.bind('ngo',NGO)
g.namespace_manager.bind('aat',AAT)
g.namespace_manager.bind('tgn',TGN)
g.namespace_manager.bind('wd',WD)
g.namespace_manager.bind('rro',RRO)
g.namespace_manager.bind('rri',RRI)
g.namespace_manager.bind('dig', DIG)
g.namespace_manager.bind('sci', SCI)

owlrl.DeductiveClosure(owlrl.OWLRL_Semantics).expand(g)

g.serialize(destination='outputs/raphael_inferenced_final.xml', format='xml')