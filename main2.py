from multiprocessing import JoinableQueue, Pool
from utils import WorkSchedule
from elasticsearch import Elasticsearch, helpers
import json
import csv
import gzip
import pathlib
import numbers

DATA_PATH = "/home/ni/data/openalex-snapshot/data"
# 最后更新的时间，作为增量更新的起点

VERSION = "2022_10_31"
MONGOURI= "mongodb://knogen:knogen@192.168.1.229"
Process_Count = 22

# 获得 merge id 排除
class Mergeids():

    project_name = "merged_ids"
    def __init__(self, basic_path:str):
        self.Projects = set(['authors','institutions','venues', 'works'])
        self.Data_Dict = {}
        self.path = pathlib.Path(basic_path).joinpath(self.project_name)
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
            print("key is not match", key)
            return set()
        return self.Data_Dict[key]
    
class Base:
    project_name="Placehold"

    def __init__(self, basic_path:str, merge_id_class: Mergeids):
        self.path = pathlib.Path(basic_path).joinpath(self.project_name)
        self.merge_id_set = merge_id_class.get_merge_id_set(self.project_name)
        self.schedule = self._init_scedule()
        self._init_elastic()

    @staticmethod
    def get_es_instance() -> Elasticsearch:
        es8 = Elasticsearch("http://192.168.1.229:9200")
        es8.ping()
        return es8

    def _init_elastic(self) -> Elasticsearch:
        
        es8 = self.get_es_instance()
        self.es_index = f'{self.project_name}_{VERSION}'
        # get mapping
        with open(f"./mapping/{self.project_name}.json",'rt')as f:
            mapping_data = json.load(f)
        with open(f"./mapping/setting.json",'rt')as f:
            setting_data = json.load(f)
        try:
            es8.indices.create(index=self.es_index,mappings=mapping_data,settings=setting_data)
        except Exception as e:
            print("build index fail,", e )
        es8.close()

    @staticmethod
    def _init_scedule() -> WorkSchedule:
        return WorkSchedule(MONGOURI)

    def _iterator_file_path(self,path: pathlib.Path):
        for sub_path in path.iterdir():
            if sub_path.is_file():
                continue
            for file_path in sub_path.iterdir():

                if file_path.is_file() and file_path.name.endswith(".gz"):
                    # scheduler
                    schedule_key = file_path.as_uri()
                    if (self.schedule.get_worker_key(schedule_key)):
                        print("file complete:",schedule_key)
                        continue

                    yield file_path

    @staticmethod
    def _shorten_url( data, keys):
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

    @staticmethod
    def _remove_empty_key(data:dict):
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

    @staticmethod
    def _remove_key(data, keys):
        for key in keys:
            if key in data:
                del data[key]

    @staticmethod
    def handle_data(file_path: pathlib.Path, cls:any, merge_id_set:set, es_index:str):
        schedule_key = file_path.as_uri()
        print("strt:", schedule_key)
        def get_data():
            with gzip.open(file_path,'rt')as f:
                for row in f:
                    data = json.loads(row)
                    if data['id'] in merge_id_set:
                        continue
                    data = cls.simplify_data(data) 
                    data['_index'] = es_index
                    data['_id'] = data['id']
                    yield data

        es8 = cls.get_es_instance()
        for success, info in helpers.parallel_bulk(es8, get_data(),2, 1000, request_timeout=360):
            if not success:
                print('A document failed:', info)
                return
        schedule = cls._init_scedule()
        schedule.set_worker_key(schedule_key)
        es8.close()
        schedule.close()
        print("test", file_path)

    def flow(self):

        todo_file_paths = []
        for path in self._iterator_file_path(self.path):
            print(path)
            todo_file_paths.append((path,self.__class__,self.merge_id_set, self.es_index))
        
        print("job size", len(todo_file_paths))
        with Pool(Process_Count) as p:
            p.starmap(self.handle_data, todo_file_paths)


    def simplify_data(self):
        pass

class Concepts(Base):

    project_name = "concepts"

    @classmethod
    def simplify_data(cls, data: dict):
        cls._shorten_url(data, ('id','wikidata'))
        cls._shorten_url(data['ids'], ('openalex','wikidata','wikipedia'))
        cls._remove_key(data,('image_url', 'image_thumbnail_url','works_api_url', 'related_concepts','created_date'))
        for row in data.get('ancestors',[]):
            cls._shorten_url(row, ('id','wikidata'))
        # for row in data.get('related_concepts',[]):
        #     cls._shorten_url(row, ('id','wikidata'))
        cls._remove_empty_key(data)
        return data

class Institutions(Base):

    project_name = "institutions"

    @classmethod
    def simplify_data(cls, data: dict):
        cls._shorten_url(data, ('id','ror','wikidata'))
        cls._shorten_url(data['ids'], ('openalex','ror','wikidata','wikipedia'))
        cls._remove_key(data,('image_url', 'image_thumbnail_url','works_api_url', 'associated_institutions','x_concepts','created_date'))
        # for row in data.get('ancestors',[]):
        #     cls._shorten_url(row, ('id','wikidata'))
        # for row in data.get('related_concepts',[]):
        #     cls._shorten_url(row, ('id','wikidata'))
        cls._remove_empty_key(data)
        cls._remove_empty_key(data.get("geo"))
        return data

class Venues(Base):

    project_name = "venues"

    @classmethod
    def simplify_data(cls, data: dict):

        cls._shorten_url(data, ('id',))
        cls._shorten_url(data['ids'], ('openalex',))
        cls._remove_key(data,('x_concepts', 'works_api_url'))
        # for row in data.get('ancestors',[]):
        #     cls._shorten_url(row, ('id','wikidata'))
        # for row in data.get('related_concepts',[]):
        #     cls._shorten_url(row, ('id','wikidata'))
        cls._remove_empty_key(data)
        return data

class Authors(Base):

    project_name = "authors"

    @classmethod
    def simplify_data(cls, data: dict):

        cls._shorten_url(data, ('id','orcid'))
        cls._shorten_url(data['ids'], ('openalex','orcid'))
        cls._shorten_url(data.get('last_known_institution'), ('id','ror'))
        cls._remove_key(data,('x_concepts', 'works_api_url', 'created_date'))
        cls._remove_empty_key(data)
        # for row in data.get('ancestors',[]):
        #     cls._shorten_url(row, ('id','wikidata'))
        # for row in data.get('related_concepts',[]):
        #     cls._shorten_url(row, ('id','wikidata'))
        return data

class Works(Base):

    project_name = "works"

    @staticmethod
    def _shorten_doi(data):
        if data.get("doi"):
            data["doi"] = data["doi"].replace("https://doi.org/", "")

    @staticmethod
    def _shorten_id_form_list(data:list):
        if not data:
            return []
        return [item.split("/")[-1] for item in data]

    @staticmethod
    def _un_abstract_inverted_index(abstract_inverted_index:dict):
        word_index = [] 
        for k,v in abstract_inverted_index.items():
            for index in v: 
                word_index.append((k,index))
        word_index = sorted(word_index,key = lambda x : x[1])
        return " ".join(map(lambda x:x[0],word_index))

    @classmethod
    def simplify_data(cls, data: dict):
        cls._shorten_doi(data)
        cls._shorten_doi(data['ids'])

        cls._shorten_url(data, ('id','orcid'))
        cls._shorten_url(data['ids'], ('openalex','pmid'))
        cls._shorten_url(data.get('host_venue'), ('id','pmid'))

        cls._remove_key(data,('title', 'ngrams_url', 'cited_by_api_url', 'created_date', 'related_works'))

        for row in data.get('authorships',[]):


            cls._shorten_url(row.get('author'), ('id','orcid'))
            cls._remove_empty_key(row.get('author'))
            # cls._shorten_url(row.get('institutions'), ('id','ror'))
            # cls._remove_empty_key(row.get('institutions'))
            for sub_row in row.get('institutions',[]):
                cls._shorten_url(sub_row, ('id','ror'))
                cls._remove_empty_key(sub_row)


            cls._remove_key(row,('raw_affiliation_string',))

            cls._remove_empty_key(row)

        for row in data.get('concepts',[]):
            cls._shorten_url(row, ('id','wikidata'))
            cls._remove_empty_key(row)

            # 减少冗余
            cls._remove_key(row,('wikidata',))

        for row in data.get('alternate_host_venues',[]):
            cls._shorten_url(row, ('id',))
            cls._remove_empty_key(row)

        if data.get('referenced_works'):
            data['referenced_works'] = cls._shorten_id_form_list(data['referenced_works'])

        # if data.get('related_works'):
        #     data['related_works'] = cls._shorten_id_form_list(data['related_works'])
        
        # 减少冗余
        cls._remove_key(data['ids'],('openalex', 'doi'))
        cls._remove_key(data['host_venue'],('issn_l', 'issn', 'url','license', 'version'))


        cls._remove_empty_key(data.get('host_venue'))
        cls._remove_empty_key(data.get('biblio'))
        cls._remove_empty_key(data.get('open_access'))
        
        cls._remove_empty_key(data)
        
        if data.get('abstract_inverted_index'):
            data['abstract'] = cls._un_abstract_inverted_index(data.get('abstract_inverted_index'))
            del(data['abstract_inverted_index'])
        
        return data

if __name__ == "__main__":

    # load_project("concepts", 

    mid = Mergeids(DATA_PATH)
    # print(mid.get_merge_id_set("authors"))

    # mid = Concepts( DATA_PATH,mid)
    # mid.flow()

    iis = Institutions( DATA_PATH, mid)
    iis.flow()

    # vus = Venues( DATA_PATH, mid)
    # vus.flow()

    # ats = Authors( DATA_PATH, mid)
    # ats.flow()
    
    # wks = Works( DATA_PATH, mid)
    # wks.flow()
    

    