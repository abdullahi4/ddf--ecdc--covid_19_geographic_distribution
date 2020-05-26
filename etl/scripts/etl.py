import pandas as pd
import os.path as osp
import re
import sys
from datetime import datetime
from inflection import humanize, underscore
from update_source import source_path
from ddf_utils.str import to_concept_id
from ddf_utils.io import cleanup, dump_json
from ddf_utils.package import get_datapackage

# config

renames = {
    'dateRep': 'day'
}
entity_columns = {
    'geoId': ['geoId', 'countriesAndTerritories','countryterritoryCode','popData2018','continentExp']
}
datapoint_key_concepts = ['geoId', 'dateRep']
time_columns = {
    'dateRep': {
        'from': '%d/%m/%Y',
        'to': '%Y%m%d'
    }
}
remove_columns = ['day','month','year']
numeric_dtypes = ['float64', 'float32', 'int32', 'int64']

script_dir = osp.abspath(osp.dirname(__file__))
output_dir = osp.join(script_dir, '..', '..')

# script 

def rename(str, renames):
    return renames[str] if str in renames else str

def concept_id(obj, renames={}, dict_value=True, dict_key=True):
    if isinstance(obj, list):
        return [concept_id(x, renames) for x in obj]
    if isinstance(obj, dict):
        return { 
            concept_id(key, renames) if dict_key else key 
            :
            concept_id(value, renames) if dict_value else value
            for (key,value) in obj.items()
        }
    return to_concept_id(underscore(rename(obj, renames)))


def ddf_table(df, key, split_datapoints=True, renames={}, id_concepts=['concept'], out_dir=output_dir):

    # deduplicating
    df = remove_duplicates(df, key)

    # sorting
    indicators = get_indicators(df, key)
    df = df.sort_values(key)
    df = df[sorted(key) + sorted(indicators)]

    # export
    if collection_type(key) == 'datapoints' and split_datapoints:
        for ind in indicators:
            split_df = df[key + [ind]]
            to_csv(split_df, key, out_dir=out_dir)
    else:
        to_csv(df, key, out_dir=out_dir)

    return df

def remove_duplicates(df, key):
    dups = df.duplicated(subset=key)
    deduped = df[~dups]
    diff = len(df.index) - len(deduped.index)
    if diff > 0:
        print(f'Dropped {diff} duplicate keys: {df[dups]}')
    return deduped

def to_csv(df, key, out_dir=output_dir):
    file_path = osp.join(out_dir, get_file_name(df, key))
    df.to_csv(file_path, index=False)

def collection_type(key):
    if len(key) > 1:
        return 'datapoints'
    elif key[0] == 'concept':
        return 'concepts'
    else:
        return 'entities'

def get_indicators(df, key):
    return list(filter(lambda col: col not in key, df.columns))

def get_file_name(df, key):
    col_type = collection_type(key)
    name = 'ddf--' + col_type
    if col_type == 'datapoints':
        indicators = get_indicators(df, key)
        name += '--' + '--'.join(indicators) + '--by--' + '--'.join(key)
    elif col_type == 'entities':
        name += '--' + key[0]
    name += '.csv'
    return name


def extract_concepts(dfs):
    concepts = set()
    concept_types = {}
    names = {}
    for df in dfs:
        for col in df.columns:
            if col not in concepts:
                concepts.add(col)
                concept_types[col] = get_concept_type(df[col])
                names[col] = humanize(col)

    return pd.DataFrame({
        'concept': list(concepts),
        'concept_type': [concept_types[c] for c in concepts],
        'name': [names[c] for c in concepts],
        'domain': ''
    })

def get_concepts_including_self(dfs):
    inferred_concepts = ['concept','concept_type']
    df = extract_concepts(dfs)
    df = pd.concat([df, extract_concepts([df.drop(columns=inferred_concepts)])])
    df = df.drop_duplicates(subset=['concept'], keep='first')
    return df


def get_concept_type(series):
    if series.name in entity_concepts.keys():
        return 'entity_domain'
    if series.name in time_concepts.keys():
        return 'time'
    if series.dtype in numeric_dtypes:
        return 'measure'
    if series.dtype == 'bool':
        return 'boolean'
    return 'string'

def reformatter_datetime(formats):
    def reformat(str):
        return datetime.strptime(str, formats['from']).strftime(formats['to'])
    return reformat

if __name__ == '__main__':

    cleanup(output_dir)

    # change config column names to concept ids
    renames = concept_id(renames, dict_key=False)
    entity_concepts = concept_id(entity_columns, renames)
    id_concepts = ['concept'] + list(entity_concepts.keys())
    time_concepts = concept_id(time_columns, renames, dict_value=False)
    datapoint_key_concepts = concept_id(datapoint_key_concepts, renames)

    # read source
    try:
        df = pd.read_csv(source_path, encoding='latin-1', keep_default_na=False)
    except pd.errors.ParserError:
        print('Could not parse source file, please check source format.', source_path)
        raise
 
    # remove and rename columns and create valid entity/concept id's
    df = df.drop(columns=remove_columns)
    df = df.rename(columns=renames)
    df = df.rename(columns=concept_id)
    df = df.apply(lambda col: 
        col.apply(concept_id) 
        if col.name in id_concepts 
        else col
    )

    # reformat time concept
    df = df.apply(lambda col: 
        col.apply(reformatter_datetime(time_concepts[col.name]))
        if col.name in time_concepts 
        else col
    )

    data_dfs = []

    # entities
    for concept in entity_concepts:
        entities = df[entity_concepts[concept]]
        entities = ddf_table(entities, key=[concept], renames=renames, id_concepts=id_concepts)
        data_dfs.append(entities)

    # datapoints
    entity_columns = sum(entity_concepts.values(),[])
    indicator_cols = filter(lambda col: col not in entity_columns or col in datapoint_key_concepts, df.columns)
    datapoints = df[indicator_cols]
    datapoints = ddf_table(datapoints, key=datapoint_key_concepts, renames=renames, id_concepts=id_concepts)
    data_dfs.append(datapoints)

    # concepts
    concepts = get_concepts_including_self(data_dfs)
    ddf_table(concepts, key=['concept'])

    # datapackage
    dp = get_datapackage(output_dir, update=True)
    dp_path = osp.join(output_dir, 'datapackage.json')
    dump_json(dp_path, dp)
