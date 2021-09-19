import requests
import pandas as pd
import re
import sqlalchemy as sa
from urllib.parse import urlencode
from tqdm import tqdm
from time import sleep

class DataCollector:
    '''Class for parsing data from headhunter's API
    
    Attributes
    ----------
    _BASE_URL: str
        basepath for API query
        
    _COLUMN_NAMES: tuple
        names for columns in resulting dataframe with vacancies
        
    '''
    _BASE_URL = 'https://api.hh.ru/vacancies/'
    _COLUMN_NAMES = (
        'id',
        'name',
        'employer',
        'from',
        'to',
        'currency',
        'description',
        'city',
        'full_address',
        'url',
        'key_skills',
        'schedule',
        'specializations',
        'published_at',
        'experience'
    )
    
    def __init__(self, params={}):
        '''
        Parameters
        ----------
        params: dict
            dictionary with parameters for api request
            check https://github.com/hhru/api/blob/master/docs/vacancies.md#item for more information
        
        '''
        self._target_url = self._BASE_URL + '?' + urlencode(params, doseq=True)
    
    @staticmethod
    def clean_html(html):
        '''Static method for cleaning string from html tags and unicode symbols 
        
        Parameters
        ----------
        html: str
            string that needs to be cleaned
            
        Returns
        -------
        str
            cleaned string without html or unicode symbols
            
        '''
        return re.sub('<[^>]*>|&[^;]*;', '', html)
    
    @staticmethod
    def read_database(database_url):
        '''Get vacancies' data from local postgres database
        
        Parameters
        ----------
        database_url: str
            local postgres database's url to connect to
        
        Returns
        -------
        vacancies: pandas.DataFrame
            all the vacancies stored in database
        
        '''
        
        engine = sa.create_engine(database_url)
        query = '''
                SELECT * FROM vacancy
                '''
        with engine.begin() as conn:
            query_result = conn.execute(query)
        column_names = query_result.keys()
        data = query_result.fetchall()
        vacancies = pd.DataFrame(data, columns=column_names).set_index('id')
        return vacancies
    
    def parse_vacancy(self, vacancy_id):
        '''Parse vacancy's details from headhunter's API
        
        Parameters
        ----------
        vacancy_id: int
            unique vacancy's id from hh.ru
            
        Returns
        -------
        tuple
            vacancy's data corresponding to columns in _COLUMN_NAMES
            
        '''
        salary = {'from': None, 'to': None, 'currency': None}
        address = {'city': None, 'raw':None}
        vacancy_url = f'{self._BASE_URL}{vacancy_id}'
        
        # try GET request 3 times to prevent bad responses
        for i in range(3):
            try:
                vacancy = requests.get(vacancy_url).json()
                break
            except:
                print(f'Bad response for vacancy with id {vacancy_id}, trying again...')
                sleep(5)
                
        # check if returned vacancy's salary and address is null
        # if not null - unpack values
        if vacancy['salary']:
            for key in salary.keys():
                salary[key] = vacancy['salary'][key]
        if vacancy['address']:
            for key in address.keys():
                address[key] = vacancy['address'][key]
        return (
            vacancy['id'],
            vacancy['name'],
            vacancy['employer']['name'],
            salary['from'],
            salary['to'],
            salary['currency'],
            self.clean_html(vacancy['description']),
            address['city'],
            address['raw'],
            vacancy['alternate_url'],
            [skill['name'] for skill in vacancy['key_skills']],
            vacancy['schedule']['name'],
            [spec['profarea_name'] for spec in vacancy['specializations']],
            vacancy['published_at'][:10],
            vacancy['experience']['name']
        )
    
    def collect_vacancies(self, store=None, postgres_url=None):
        '''Collect, parse and store first 2000 (maximum possible value via API)
        vacancies correspoinding to API query in self._target_url
        
        Parameters
        ----------
        store: str
            how to store vacancies returned by headhunter's API
            possible values: 'csv', 'postgres', else return 
            vacancies without storing.
        
        postgres_url: str
            local postgres database's url to connect to in format
            'postgresql://{username}:{password}@{host}:{port}/{database}',
            must be specified if store='postgres'
        
        Returns
        -------
        vacancies: pandas.DataFrame
            pandas dataframe with columns corresponding to _COLUMN_NAMES 
            and rows being vacancy's data from parse_vacancy function
        
        '''
        
        # collect ids of all vacancies corresponding to API query
        ids = []      
        num_pages = requests.get(self._target_url).json()["pages"]
        for page in range(num_pages):
            data = requests.get(self._target_url, params={'page': page}).json()['items']
            ids.extend([vacancy['id'] for vacancy in data])
        
        # collect data on vacancies in ids list and construct the vacancies dataframe 
        vacancies_iter = map(lambda x: self.parse_vacancy(x), ids)
        rows = [vacancy for vacancy in tqdm(vacancies_iter, total=len(ids))]
        vacancies = pd.DataFrame(rows, columns=self._COLUMN_NAMES).set_index('id')
        
        if store == 'csv':
            vacancies.to_csv('vacancies.csv')
            
        elif store == 'postgres':
            # insert data into local postgresql database that is not in database already
            dtypes = (sa.INT(), sa.String(length=255), sa.String(length=255), sa.INT(), sa.INT(), sa.String(length=10), sa.Text(), 
                    sa.String(length=100), sa.Text(), sa.Text(), sa.ARRAY(sa.Text()), sa.Text(), sa.ARRAY(sa.Text()), sa.Date(), sa.Text())
            cols_dtypes = {k:v for k,v in zip(self._COLUMN_NAMES, dtypes)}

            engine = sa.create_engine(postgres_url)
            vacancies.to_sql('temp_vacancy', engine, if_exists='replace', dtype=cols_dtypes)
            query = '''
                    INSERT INTO vacancy
                    SELECT * FROM temp_vacancy AS tv
                    WHERE tv.id not in (SELECT v.id FROM vacancy AS V)
                    '''
            with engine.begin() as conn:
                conn.execute(query) 
        return vacancies
    
