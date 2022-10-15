from dataclasses import replace
from elasticsearch import Elasticsearch, helpers
import json
import csv
import gzip
import pathlib
import numbers

DATA_PATH = "/home/ni/data/openAlex/data"
# 最后更新的时间，作为增量更新的起点
LAST_UPDATE_FLAG_FILE = "/tmp/openaliex_update_flag.txt"
VERSION = "2022_10_12"

# 读取 last flag
def get_last_update_flag()-> str: 
    flag = "2020-01-01"
    try:
        with open(LAST_UPDATE_FLAG_FILE, 'rt')as f:
            data = f.read()
            if data:
                flag = data.replace("updated_date=","")
    except FileNotFoundError:
        pass
    return flag

# 保存 last flag
def set_last_update_flag(flag: str):
    with open(LAST_UPDATE_FLAG_FILE, 'wt')as f:
        f.write(flag)

# def get_mergeid():

def load_project(name: str, last_flag: str):
    project_path = pathlib.Path(DATA_PATH).joinpath(name)
    for path in project_path.iterdir():
        flag = path.name.split('=')[-1]
        if flag <= last_flag:
            continue
        print(path)

class base:
    project_name="Placehold"
    def __init__(self, last_flag: str, basic_path:str):
        self.path = pathlib.Path(basic_path).joinpath(self.project_name)
        self.last_flag = last_flag
        
    def _init_elastic(self):
        
        self.es8 = Elasticsearch("http://192.168.1.229:9200")
        self.es8.ping()
        self.es_index = f'{self.project_name}_{VERSION}'

        # get mapping
        with open(f"./mapping/{self.project_name}.json",'rt')as f:
            mapping_data = json.load(f)
        with open(f"./mapping/setting.json",'rt')as f:
            setting_data = json.load(f)
        self.es8.indices.create(index=self.es_index,mappings=mapping_data,settings=setting_data)
        # self.es8.indices.create(index=self.es_index)
        # self.es8.indices.put_mapping(mapping_data, index=self.es_index)
        # self.es8.indices.put_settings(index=self.es_index, settings={
        #     "index.mapping.total_fields.limit": 100000    
        # })

    def _iterator_file_path(self,path: pathlib.Path):
        for sub_path in path.iterdir():
            print("start sub_path", sub_path.name)
            if sub_path.is_file():
                continue
            flag = sub_path.name.split('=')[-1]
            if flag <= self.last_flag:
                continue
            for file_path in sub_path.iterdir():
                if file_path.is_file() and file_path.name.endswith(".gz"):
                    yield file_path

    def _shorten_url(self, data, keys):
        if not data:
            return
        for key in keys:
            if key in data and data[key]:
                try:
                    data[key] = data[key].split("/")[-1]
                except Exception as e:
                    # bug dirty fix because the source data
                    if key == "id" and isinstance(data[key],numbers.Number):
                        data[key] = str(data[key])
                        print("change id type number to str", data[key])
                    else:
                        print("ignore. shorten url fail",key, e)

    def _remove_empty_key(self,data:dict):
        if not data:
            return
        empty_keys = []
        for key,value in data.items():
            if isinstance(value, (numbers.Number,bool)):
                continue 
            if not value:
                empty_keys.append(key)
        for key in empty_keys:
            del data[key]

    def _remove_key(self, data, keys):
        for key in keys:
            if key in data:
                del data[key]

    def simplify_data(self):
        pass
            
# 获得 merge id 排除
class Mergeids(base):

    project_name = "merged_ids"
    def __init__(self, last_flag: str, basic_path:str):
        super(Mergeids,self).__init__(last_flag, basic_path)
        self.Projects = set(['authors','institutions','venues', 'works'])
        self.Data_Dict = {}

        self.handle()

    def _iterator_file_path(self,path: pathlib.Path):
        for file_path in path.iterdir():
            if file_path.is_file() and file_path.name.endswith(".gz"):
                yield file_path

    def handle(self):
        for project_name in self.Projects:
            self.Data_Dict[project_name] = set()
            for file_path in self._iterator_file_path(self.path.joinpath(project_name)):
                with gzip.open(file_path,'rt')as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        self.Data_Dict[project_name].add(row['id'])

    def get_merge_id_set(self,key):
        if key not in self.Projects:
            print("key is not match")
            return
        return self.Data_Dict[key]
    

class Concepts(base):

    project_name = "concepts"
    def __init__(self, last_flag: str, basic_path:str):
        super().__init__(last_flag, basic_path)
        self._init_elastic()

    def handle_data(self):
        for file_path in self._iterator_file_path(self.path):
            with gzip.open(file_path,'rt')as f:
                for row in f:
                    data = json.loads(row)
                    data = self.simplify_data(data) 
                    # print(data)
                    data['_index'] = self.es_index
                    data['_id'] = data['id']
                    yield data

    def flow(self):
        for success, info in helpers.parallel_bulk(self.es8, self.handle_data(),5,1000):
            if not success:
                print('A document failed:', info)

    def simplify_data(self, data: dict):
        self._shorten_url(data, ('id','wikidata'))
        self._shorten_url(data['ids'], ('openalex','wikidata','wikipedia'))
        self._remove_key(data,('image_url', 'image_thumbnail_url','works_api_url', 'related_concepts','created_date'))
        for row in data.get('ancestors',[]):
            self._shorten_url(row, ('id','wikidata'))
        # for row in data.get('related_concepts',[]):
        #     self._shorten_url(row, ('id','wikidata'))
        self._remove_empty_key(data)
        return data


class Institutions(base):

    project_name = "institutions"
    def __init__(self, last_flag: str, basic_path:str, merge_id_class: Mergeids):
        super().__init__(last_flag, basic_path)
        self._init_elastic()
        self.merge_id_set = merge_id_class.get_merge_id_set(self.project_name)

    def handle_data(self):
        for file_path in self._iterator_file_path(self.path):
            with gzip.open(file_path,'rt')as f:
                for row in f:
                    data = json.loads(row)
                    if data['id'] in self.merge_id_set:
                        continue
                    data = self.simplify_data(data) 
                    # print(data)
                    data['_index'] = self.es_index
                    data['_id'] = data['id']
                    yield data

    def flow(self):
        for success, info in helpers.parallel_bulk(self.es8, self.handle_data(),5,1000):
            if not success:
                print('A document failed:', info)

    def simplify_data(self, data: dict):
        self._shorten_url(data, ('id','ror','wikidata'))
        self._shorten_url(data['ids'], ('openalex','ror','wikidata','wikipedia'))
        self._remove_key(data,('image_url', 'image_thumbnail_url','works_api_url', 'associated_institutions','x_concepts','created_date'))
        # for row in data.get('ancestors',[]):
        #     self._shorten_url(row, ('id','wikidata'))
        # for row in data.get('related_concepts',[]):
        #     self._shorten_url(row, ('id','wikidata'))
        self._remove_empty_key(data)
        self._remove_empty_key(data.get("geo"))
        return data


class Venues(base):

    project_name = "venues"
    def __init__(self, last_flag: str, basic_path:str, merge_id_class: Mergeids):
        super().__init__(last_flag, basic_path)
        self._init_elastic()
        self.merge_id_set = merge_id_class.get_merge_id_set(self.project_name)

    def handle_data(self):
        for file_path in self._iterator_file_path(self.path):
            with gzip.open(file_path,'rt')as f:
                for row in f:
                    data = json.loads(row)
                    if data['id'] in self.merge_id_set:
                        continue
                    data = self.simplify_data(data) 
                    # print(data)
                    data['_index'] = self.es_index
                    data['_id'] = data['id']
                    yield data

    def flow(self):
        for success, info in helpers.parallel_bulk(self.es8, self.handle_data(),5,1000):
            if not success:
                print('A document failed:', info)

    def simplify_data(self, data: dict):

        self._shorten_url(data, ('id',))
        self._shorten_url(data['ids'], ('openalex',))
        self._remove_key(data,('x_concepts', 'works_api_url'))
        # for row in data.get('ancestors',[]):
        #     self._shorten_url(row, ('id','wikidata'))
        # for row in data.get('related_concepts',[]):
        #     self._shorten_url(row, ('id','wikidata'))
        self._remove_empty_key(data)
        return data


class Authors(base):

    project_name = "authors"
    def __init__(self, last_flag: str, basic_path:str, merge_id_class: Mergeids):
        super().__init__(last_flag, basic_path)
        self._init_elastic()
        self.merge_id_set = merge_id_class.get_merge_id_set(self.project_name)

    def handle_data(self):
        for file_path in self._iterator_file_path(self.path):
            with gzip.open(file_path,'rt')as f:
                for row in f:
                    data = json.loads(row)
                    if data['id'] in self.merge_id_set:
                        continue
                    data = self.simplify_data(data) 
                    # print(data)
                    data['_index'] = self.es_index
                    data['_id'] = data['id']
                    yield data

    def flow(self):
        for success, info in helpers.parallel_bulk(self.es8, self.handle_data(),5,1000):
            if not success:
                print('A document failed:', info)

    def simplify_data(self, data: dict):

        self._shorten_url(data, ('id','orcid'))
        self._shorten_url(data['ids'], ('openalex','orcid'))
        self._shorten_url(data.get('last_known_institution'), ('id','ror'))
        self._remove_key(data,('x_concepts', 'works_api_url', 'created_date'))
        self._remove_empty_key(data)
        # for row in data.get('ancestors',[]):
        #     self._shorten_url(row, ('id','wikidata'))
        # for row in data.get('related_concepts',[]):
        #     self._shorten_url(row, ('id','wikidata'))
        return data


class Works(base):

    project_name = "works"
    def __init__(self, last_flag: str, basic_path:str, merge_id_class: Mergeids):
        super().__init__(last_flag, basic_path)
        self._init_elastic()
        self.merge_id_set = merge_id_class.get_merge_id_set(self.project_name)

    def handle_data(self):
        for file_path in self._iterator_file_path(self.path):
            print("start file", file_path.name)
            with gzip.open(file_path,'rt')as f:
                for row in f:
                    data = json.loads(row)
                    if data['id'] in self.merge_id_set:
                        continue
                    try:
                        data = self.simplify_data(data) 
                        # print(data)
                        data['_index'] = self.es_index
                        data['_id'] = data['id']
                        yield data
                    except Exception as e:
                        print("format error", e)
                        print(data)

    def flow(self):
        for success, info in helpers.parallel_bulk(self.es8, self.handle_data(),5,1000):
            if not success:
                print('A document failed:', info)
    
    def _shorten_doi(self, data):
        if data.get("doi"):
            data["doi"] = data["doi"].replace("https://doi.org/", "")

    def _shorten_id_form_list(self, data:list):
        if not data:
            return []
        return [item.split("/")[-1] for item in data]

    def _un_abstract_inverted_index(self, abstract_inverted_index:dict):
        word_index = [] 
        for k,v in abstract_inverted_index.items():
            for index in v: 
                word_index.append((k,index))
        word_index = sorted(word_index,key = lambda x : x[1])
        return " ".join(map(lambda x:x[0],word_index))

    def simplify_data(self, data: dict):
        self._shorten_doi(data)
        self._shorten_doi(data['ids'])

        self._shorten_url(data, ('id','orcid'))
        self._shorten_url(data['ids'], ('openalex','pmid'))
        self._shorten_url(data.get('host_venue'), ('id','pmid'))

        self._remove_key(data,('title', 'ngrams_url', 'cited_by_api_url', 'created_date', 'related_works'))

        for row in data.get('authorships',[]):


            self._shorten_url(row.get('author'), ('id','orcid'))
            self._remove_empty_key(row.get('author'))
            # self._shorten_url(row.get('institutions'), ('id','ror'))
            # self._remove_empty_key(row.get('institutions'))
            for sub_row in row.get('institutions',[]):
                self._shorten_url(sub_row, ('id','ror'))
                self._remove_empty_key(sub_row)


            self._remove_key(row,('raw_affiliation_string',))

            self._remove_empty_key(row)

        for row in data.get('concepts',[]):
            self._shorten_url(row, ('id','wikidata'))
            self._remove_empty_key(row)

            # 减少冗余
            self._remove_key(row,('wikidata',))

        for row in data.get('alternate_host_venues',[]):
            self._shorten_url(row, ('id',))
            self._remove_empty_key(row)

        if data.get('referenced_works'):
            data['referenced_works'] = self._shorten_id_form_list(data['referenced_works'])

        # if data.get('related_works'):
        #     data['related_works'] = self._shorten_id_form_list(data['related_works'])
        
        # 减少冗余
        self._remove_key(data['ids'],('openalex', 'doi'))
        self._remove_key(data['host_venue'],('issn_l', 'issn', 'url','license', 'version'))


        self._remove_empty_key(data.get('host_venue'))
        self._remove_empty_key(data.get('biblio'))
        self._remove_empty_key(data.get('open_access'))
        
        self._remove_empty_key(data)
        
        if data.get('abstract_inverted_index'):
            data['abstract'] = self._un_abstract_inverted_index(data.get('abstract_inverted_index'))
            del(data['abstract_inverted_index'])
        
        return data

if __name__ == "__main__":
    last_flag = get_last_update_flag()
    print("last_flag", last_flag)
    # load_project("concepts", last_flag)

    mid = Mergeids(last_flag, DATA_PATH)
    # print(mid.get_merge_id_set("authors"))

    # mid = Concepts(last_flag, DATA_PATH)
    # mid.flow()

    # iis = Institutions(last_flag, DATA_PATH, mid)
    # iis.flow()

    # vus = Venues(last_flag, DATA_PATH, mid)
    # vus.flow()

    # ats = Authors(last_flag, DATA_PATH, mid)
    # ats.flow()
    
    wks = Works(last_flag, DATA_PATH, mid)
    wks.flow()
    

    