import mysql.connector
from openpyxl import load_workbook
import random, urllib, json, string, csv
from SPARQLWrapper import SPARQLWrapper, JSON
import time

from pdb import set_trace as st

def connect_to_sql(db="grounds_sshoc"):
    mydb = mysql.connector.connect(
        host="round4",
        user="sshoc",
        password="IqazZuKaXMmSEqUl",
        database=db
    )

    return mydb

def get_json(url):
    operUrl = urllib.request.urlopen(url)
    if(operUrl.getcode()==200):
       data = operUrl.read()
       json_data = json.loads(data)
    else:
       print("Error receiving data", operUrl.getcode())
    return json_data

def find_gallery_PID(ng_number):
    ng_number = str(ng_number)
    if ng_number.startswith('https'):
        ng_number = str(ng_number.rsplit('/',1)[-1])
    else:
        ng_number = ng_number.replace(' ','_')

    gallery_PID = None

    try:
        export_url = 'https://collectiondata.ng-london.org.uk/es/ng-public/_search?q=identifier.object_number:' + ng_number
        json = get_json(export_url)
        if json['hits']['total'] > 0:
            gallery_PID = json['hits']['hits'][0]['_id']
    except:
        gallery_PID = None

    return gallery_PID

def check_db(input_literal, table_name):
    db = connect_to_sql()
    cursor = db.cursor()
    pid_value = None
    input_literal = str(input_literal)
    if table_name == 'temp_pids':
        val = 'pid_value'
        str_input = 'literal_value'
    elif table_name == 'wikidata':
        val = 'wd_value'
        str_input = 'string_literal'

    query = "SELECT " + val + " FROM sshoc." + table_name + " WHERE " + str_input + " = %s"
    cursor.execute(query, (input_literal,))
    result = cursor.fetchall()
    
    if len(result) > 0:
        pid_value = result[0][0]

    return pid_value

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
    old_pid = find_gallery_PID(input_literal)
    
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

def get_property(uri):
    remove_uri = uri.replace('https://rdf.ng-london.org.uk/raphael/resource/','')
    final_property = remove_uri.replace('_',' ')
    if '.' in final_property:
        final_property = str(final_property.split('.')[1])
    if 'RRR' in final_property:
        final_property = final_property.replace('RRR','')
    return final_property

def find_aat_value(material,material_type):
    if 'https://rdf.ng-london.org.uk/raphael/resource/' in material:
        material = str(get_property(material))
    else:
        material = str(material)
    
    if material_type == 'medium':
        wb = load_workbook(filename = 'inputs/NG_Meduim_and_Support_AAT.xlsx', read_only=True)
        ws = wb['Medium Material']
    elif material_type == 'support':
        wb = load_workbook(filename = 'inputs/NG_Meduim_and_Support_AAT.xlsx', read_only=True)
        ws = wb['Support Materials']
    elif material_type == 'roles':
        wb = load_workbook(filename = 'inputs/NG_Roles_AAT.xlsx', read_only=True)
        ws = wb['AAT_Roles']
    elif material_type == 'material_grounds':
        wb = load_workbook(filename = 'inputs/NG_Meduim_and_Support_AAT.xlsx', read_only=True)
        ws = wb['GROUNDS materials']
    elif material_type == 'protocols':
        wb = load_workbook(filename = 'inputs/Protocol_AAT.xlsx', read_only=True)
        ws = wb['Protocol']
    elif material_type == 'techniques':
        wb = load_workbook(filename = 'inputs/Protocol_AAT.xlsx', read_only=True)
        ws = wb['Techniques']
    for row in ws.iter_rows(values_only=True):
        if material in row:
            aat_dict = {}
            aat_number_string = str(row[1])
            aat_number = aat_number_string.replace('aat:','')
            aat_type = row[2]
            aat_dict.update({aat_number:aat_type})
            for x in range(3, 9, 2):
                try:
                    aat_number_string = str(row[x])
                    aat_number = aat_number_string.replace('aat:','')
                    aat_type = row[x+1]
                    aat_dict.update({aat_number:aat_type})
                except:
                    pass
            return aat_dict
    wb.close()

def run_ruby_program(input_string):
    import subprocess

    ruby_var = 'ruby citation_parser.rb \'' + input_string + '\''
    output = subprocess.Popen(ruby_var, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, error = output.communicate()

    try:
        string_output = out.decode("utf-8")
        json_output = json.loads(string_output)
    except:
        return

    return json_output

def wikidata_query(literal, literal_type, db):
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
        thing_type = 'Q515'
        string_literal = str(literal)
    elif literal_type == 'language':
        thing_type = 'Q34770'
        string_literal = str(literal)
    
    result = check_db(string_literal, 'wikidata')
    
    if result is not None:
        result = result.replace('http://www.wikidata.org/entity/','')
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
        
        if ret["results"]["bindings"] != []:
            for result in ret["results"]["bindings"]:
                #db = connect_to_sql()
                cursor = db.cursor()
                result = result["year"]["value"]
                query = 'INSERT INTO sshoc.wikidata (string_literal, wd_value) VALUES (%s, %s)'
                val = (string_literal, result)
                cursor.execute(query, val)
                db.commit()
        else:
            #db = connect_to_sql()
            cursor = db.cursor()
            query = 'INSERT INTO sshoc.wikidata (string_literal, wd_value) VALUES (%s, %s)'
            val = (string_literal, 'No WD value')
            cursor.execute(query, val)
            db.commit()
        result = result.replace('http://www.wikidata.org/entity/','')
        return result

def create_year_dates(year):
    import datetime
    year = int(year)

    start = datetime.datetime(year, 1, 1)
    end = datetime.datetime(year, 12, 31)

    return start, end

def check_aat_values(col_name):
    aat_db = connect_to_sql(db="sshoc")
    cursor = aat_db.cursor()
    query = "SELECT aat_title, aat_value, aat_unit_title, aat_unit_value FROM aat_refs_grounds WHERE aat_refs_grounds.dimension_column_ref = '" + col_name + "'"
    cursor.execute(query)
    result = cursor.fetchall()

    aat_title, aat_value, aat_unit_title, aat_unit_value = [x for x in result[0]]

    return aat_title, aat_value, aat_unit_title, aat_unit_value

def process_name_prefixes(row):

    if row["person_prefix_name"] is not np.nan:
        value = str(row["person_prefix_name"]) + " " + row["person_name"]
    else:
        value = row["person_name"]

    return value