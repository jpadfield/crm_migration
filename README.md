# SSHOC - Mapping Raphael to the CIDOC CRM
Please note the content of the Inputs and Outputs is licensed under a
[Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License][cc-by-nc-sa].

[![CC BY-NC-SA 4.0][cc-by-nc-sa-shield]][cc-by-nc-sa]

[![CC BY-NC-SA 4.0][cc-by-nc-sa-image]][cc-by-nc-sa]

[cc-by-nc-sa]: http://creativecommons.org/licenses/by-nc-sa/4.0/
[cc-by-nc-sa-image]: https://licensebuttons.net/l/by-nc-sa/4.0/88x31.png
[cc-by-nc-sa-shield]: https://img.shields.io/badge/License-CC%20BY--NC--SA%204.0-lightgrey.svg

In 2007 the <a href="https://cima.ng-london.org.uk/documentation">Raphael Research Resource</a> project began to examine how complex conservation, scientific and art historical research could be combined in a flexible digital form. Exploring the presentation of interrelated high resolution images and text, along with how the data could be stored in relation to an event driven ontology in the form of <a href="http://www.w3.org/TR/rdf-concepts/">RDF triples</a>. The original <a href="https://cima.ng-london.org.uk/documentation">main user interface</a> is still live and the data stored within the system has been mapped tot he CIDOC CRM using the code presented here. The SSHOC work aimed to make this data more [FAIR](https://www.go-fair.org/fair-principles/) so in addition to mapping it to a standard ontology, to increase Interoprability, it has also been made availbale in the form of <a href="http://en.wikipedia.org/wiki/Linked_Data">open linkable data</a> combined with a <a href="http://en.wikipedia.org/wiki/SPARQL">SPARQL</a> end-point. This live data presentation can been found [Here](https://rdf.ng-london.org.uk/sshoc/).

This repo contains code that parses an old XML export of the Raphael Research Resource, along with pulling additional data from some of the National Gallery internal data sources and maps the data to CIDOC-CRM. As the code currently connects to some internal systems some aditional work may be required for it to run externally.

## Running the code
* To run the code in this repo simply run '''raphael_mapping.py''' from the command line. 
* Certain inputs (AAT references) appear in the inputs folder as Excel spreadsheets; others are inclluded as XML files.
* Copies of each section of the code are saved in the outputs folder as they are generated. There are seven subgraphs which combined make up a full graph. Outputs are saved in XML, TTL and TRIG formats.
* A partial rebuild can be initiated by deleting the relevant files in the outputs folder and rerunning the code (e.g. to rebuild all objects, delete the grounds_object files). The code checks for these output files before it decides which parts of the graph to build.
* A full rebuild can be initiated by passing the command line argument 'fullrebuild' when running grounds_mapping. This will take a long time to run so it's not recommended unless absolutely necessary.
* Run inferencing.py with a graph TTL file as the input to output an inferenced version of the RDF graph.

# Acknowledgement
This project was developed and tested as part of the work of the following project:

## H2020 EU project [SSHOC](https://sshopencloud.eu/)
<img height="64px" src="https://github.com/jpadfield/simple-site/blob/master/docs/graphics/sshoc-logo.png" alt="SSHOC Grant Info">
<img height="32px" src="https://github.com/jpadfield/simple-site/blob/master/docs/graphics/sshoc-eu-tag2.png" alt="SSHOC Grant Info">
