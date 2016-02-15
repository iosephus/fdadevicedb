
import sys
import os
import urllib
import glob
import zipfile
import io
import pandas as pd
import datetime
import codecs
import sqlalchemy as sqla
from sqlalchemy.ext import declarative as sqladeclarative
import time
import math
import argparse as argp
import itertools
from nltk.util import ngrams as nltk_ngrams
import re
from collections import namedtuple

# For direct use with SQLite3, not in use
sql_create_table_pmn_applicant = '''CREATE TABLE pmn_applicant (
    applicantid INTEGER PRIMARY KEY ASC,
    applicant VARCHAR(128) NOT NULL,
    contact VARCHAR(64),
    street1 VARCHAR(128),
    street2 VARCHAR(128),
    city VARCHAR(128),
    state CHAR(2),
    zipcode VARCHAR(16),
    postalcode VARCHAR(16),
    countrycode CHAR(2)
)'''

# For direct use with SQLite3, not in use
sql_create_table_pmn_request = '''CREATE TABLE pmn_request (
    requestid INTEGER PRIMARY KEY ASC,
    knumber VARCHAR(16) NOT NULL,
    applicantid INTEGER REFERNCES REFERENCES pmn_applicant(applicantid),
    datereceived CHAR(10) NOT NULL,
    decisiondate CHAR(10) NOT NULL,
    decision VARCHAR(4) NOT NULL,
    reviewadvisecomm CHAR(2) NOT NULL,
    productcode CHAR(3),
    stateorsumm VARCHAR(9),
    classadvisecomm CHAR(2),
    type VARCHAR(11),
    thirdparty BOOLEAN,
    expeditedreview BOOLEAN,
    devicename VARCHAR(512)
)'''

# This is what's really used: SQLAlchemy
Base = sqladeclarative.declarative_base()

class PMNApplicant(Base):
    __tablename__ = 'pmn_applicant'
    applicantid = sqla.Column(sqla.Integer, primary_key=True)
    applicant = sqla.Column(sqla.String(128), nullable=False)
    contact = sqla.Column(sqla.String(64))
    street1 = sqla.Column(sqla.String(128))
    street2 = sqla.Column(sqla.String(128))
    city = sqla.Column(sqla.String(128))
    state = sqla.Column(sqla.String(2))
    zipcode = sqla.Column(sqla.String(16))
    postalcode = sqla.Column(sqla.String(16))
    countrycode = sqla.Column(sqla.String(2))

class PMNRequest(Base):
    __tablename__ = 'pmn_request'
    requestid = sqla.Column(sqla.Integer, primary_key=True)
    knumber = sqla.Column(sqla.String(16), nullable=False)
    applicantid = sqla.Column(sqla.Integer, sqla.ForeignKey('pmn_applicant.applicantid'))
    datereceived = sqla.Column(sqla.Date)
    decisiondate = sqla.Column(sqla.Date)
    decision = sqla.Column(sqla.String(4), nullable=False)
    reviewadvisecomm = sqla.Column(sqla.String(2), nullable=False)
    productcode = sqla.Column(sqla.String(3))
    stateorsumm = sqla.Column(sqla.String(9))
    classadvisecomm = sqla.Column(sqla.String(2))
    devicetype = sqla.Column(sqla.String(11))
    thirdparty = sqla.Column(sqla.Boolean)
    expeditedreview = sqla.Column(sqla.Boolean)
    devicename = sqla.Column(sqla.String(512))


columns_request_df = ['knumber',
                      'datereceived',
                      'decisiondate',
                      'decision',
                      'reviewadvisecomm',
                      'productcode',
                   'stateorsumm',
                   'classadvisecomm',
                   'devicetype',
                   'thirdparty',
                   'expeditedreview',
                   'devicename']

columns_company_df = ['applicant',
                      'street1',
                      'street2',
                      'city',
                      'state',
                      'countrycode',
                      'zipcode',
                      'postalcode']

columns_applicant_df = columns_company_df + ['contact']

fda_url_pmn_lastmonth = [
    "http://www.accessdata.fda.gov/premarket/ftparea/pmnlstmn.zip"
]

fda_url_pmn_1996curr = [
    "http://www.accessdata.fda.gov/premarket/ftparea/pmn96cur.zip"
]

fda_url_pmn_pre1996 = [
    'http://www.accessdata.fda.gov/premarket/ftparea/pmn9195.zip',
    'http://www.accessdata.fda.gov/premarket/ftparea/pmn8690.zip',
    'http://www.accessdata.fda.gov/premarket/ftparea/pmn8185.zip',
    'http://www.accessdata.fda.gov/premarket/ftparea/pmn7680.zip'
]

fda_url_pmn_all = fda_url_pmn_pre1996 + fda_url_pmn_1996curr

fda_url_pma = ["http://www.accessdata.fda.gov/premarket/ftparea/pma.zip"]
text_encoding = 'cp1252'

def read_zipped_csv(fp, encoding=text_encoding):
    """Reads all CSVs found inside a FDA downloaded Zip file. Returns single merged data frame"""
    zf = zipfile.ZipFile(fp)
    contents = [pd.read_csv(io.StringIO(codecs.decode(zf.read(name), encoding).replace('\0', '')), sep='|') for name in zf.namelist()]
    return pd.concat(contents)

def download_zipped_data_singleurl(url, encoding=text_encoding):
    """Downloads zip file from url and return data frame. Zipped file in URL
    should contain CSV files"""
    response = urllib.request.urlopen(url)
    compressedFile = io.BytesIO(response.read())
    data_frame = read_zipped_csv(compressedFile, encoding)
    return(data_frame)

def download_zipped_data(url_list, encoding=text_encoding):
    """Downloads zip files from a list of url and return data frame. Zipped
    files in each URL should contain CSV files"""
    contents = [download_zipped_data_singleurl(url, encoding) for url in url_list]
    data_frame = pd.concat(contents)
    return(data_frame.drop_duplicates())

# For direct use with SQLite3, not in use
def get_applicant_id(conn, applicant):
    applicant_dict = dict(applicant)
    condition_str = ' and '.join(['=='.join((target, repr(value))) if pd.notnull(value) else ' '.join((target, 'IS NULL')) for target, value in applicant_dict.items()])
    sql = 'SELECT applicantid FROM pmn_applicant WHERE %s' % condition_str
    cursor = conn.execute(sql)
    result = cursor.fetchone()
    if result is not None:
        applicantid = result[0]
    else:
        applicant_dict = dict((k, v) for k, v in applicant_dict.items() if pd.notnull(v))
        names = ', '.join(applicant_dict.keys())
        values = ', '.join([repr(v) for v in applicant_dict.values()])
        sql = "INSERT INTO pmn_applicant (%s) VALUES (%s)" % (names, values)
        cursor = conn.execute(sql)
        conn.commit()
        applicantid = cursor.lastrowid
    return(applicantid)

# For direct use with SQLite3, not in use
def insert_record(conn, row):
    request = row[columns_request_df]
    applicant = row[columns_applicant_df]
    applicantid = get_applicant_id(conn, applicant)
    request_dict = dict(request[pd.notnull(request)])
    names = ', '.join(list(request_dict.keys()) + ['applicantid'])
    values = ', '.join([repr(v) for v in request_dict.values()] + [repr(applicantid)])
    sql = "INSERT INTO pmn_request (%s) VALUES (%s)" % (names, values)
    conn.execute(sql)
    conn.commit()
    
def translate_column_names(columns):
    new_names = [name.lower().replace('_', '') for name in columns]
    new_names = [name if name != 'zip' else 'zipcode' for name in new_names]
    new_names = [name if name != 'type' else 'devicetype' for name in new_names]
    return(new_names)

def download_fda_data(url_list):
    d = download_zipped_data(url_list)
    d.drop("SSPINDICATOR", inplace=True, axis=1)
    d = d.where(pd.notnull(d), None)
    d.columns = translate_column_names(d.columns)
    d.datereceived = pd.to_datetime(d.datereceived)
    d.decisiondate = pd.to_datetime(d.decisiondate)
    d.loc[d.thirdparty == 'Y', 'thirdparty'] = True
    d.loc[d.thirdparty == 'N', 'thirdparty'] = False
    d.loc[d.expeditedreview == 'Y', 'expeditedreview'] = True
    d.loc[d.expeditedreview == 'N', 'expeditedreview'] = False
    return(d)

def create_database(engine, dataframe):
    Base.metadata.create_all(engine)
    applicants = dataframe[columns_applicant_df].drop_duplicates()
    applicants.to_sql('pmn_applicant', engine, if_exists='append', index=False)
    applicant_readback = pd.read_sql_table('pmn_applicant', engine)
    dataframe_merged = dataframe.merge(applicant_readback, on=columns_applicant_df, how='left')
    dataframe_merged_tostore = dataframe_merged[columns_request_df + ['applicantid']].sort_values(by='decisiondate')
    dataframe_merged_tostore.to_sql('pmn_request', engine, if_exists='append', index=False)


def get_applicant_name_text(row):
    text = str(row.applicant).lower()
    unigrams = re.split('[\s]+', text)
    last_word = unigrams[-1].replace('.', '')
    if last_word in ['corp', 'inc', 'llc', 'co', 'ltd', 'gmbh']:
        return unigrams[:-1]
    else:
        return(unigrams)

def get_applicant_contact_text(row):
    text = str(row.contact).lower()
    text = re.sub('[^A-Za-z-]', '', text)
    unigrams = re.split('[\s]+', text)
    if last_word in ['phd', 'md', 'dds', 'jr', 'ii', 'iii']:
        return unigrams[:-1]
    else:
        return(unigrams)

def get_applicant_address_text(row):
    entries = [str(e) for e in row[['street1', 'street2', 'city']] if e is not None]
    if row.countrycode == 'US' and row.zipcode is not None:
        entries = entries + [str(row.zipcode)[0:5]]
    if row.countrycode != 'US' and row.postalcode is not None:
        entries = entries + [str(row.postalcode)]
    text = ' '.join(entries).lower()
    text = re.sub('[^a-z0-9]', ' ', text)
    unigrams = re.split('[\s]+', text)
    return(unigrams)

def get_ngrams(unigrams, orders=[1, 2]):
    all_ngrams = itertools.chain(*map(lambda n: list(nltk_ngrams(unigrams, n)), orders))
    return(set(all_ngrams))

def compute_dice_coefficient(s1, s2):
    dice = 2.0 * len(s1.intersection(s2)) / (len(s1) + len(s2))
    return(dice)

def compute_applicant_match_p(df, i1, i2):
    row1 = df.ix[i1]
    row2 = df.ix[i2]
    if row1.countrycode != row2.countrycode:
        return(0.0)
    if row1.countrycode == 'US' and row1.state != row2.state:
        return(0.0)
    s1_name = get_ngrams(get_applicant_name_text(row1))
    s2_name = get_ngrams(get_applicant_name_text(row2))
    dice_name = compute_dice_coefficient(s1_name, s2_name)
    if dice_name < 0.1:
        return(0.0)
    s1_addr = get_ngrams(get_applicant_address_text(row1))
    s2_addr = get_ngrams(get_applicant_address_text(row2))
    dice_addr = compute_dice_coefficient(s1_addr, s2_addr)
    if dice_addr < 0.1:
        return(0.0)
    dice = (0.5 * dice_name + 0.5 * dice_addr)
    return(dice)


def compute_applicant_match_p_wcontact(df, i1, i2):
    row1 = df.ix[i1]
    row2 = df.ix[i2]
    if row1.countrycode != row2.countrycode:
        return(0.0)
    if row1.countrycode == 'US' and row1.state != row2.state:
        return(0.0)
    s1_name = get_ngrams(get_applicant_name_text(df.ix[i1]))
    s2_name = get_ngrams(get_applicant_name_text(df.ix[i2]))
    dice_name = compute_dice_coefficient(s1_name, s2_name)
    if dice_name < 0.1:
        return(0.0)
    s1_addr = get_ngrams(get_applicant_address_text(df.ix[i1]))
    s2_addr = get_ngrams(get_applicant_address_text(df.ix[i2]))
    dice_addr = compute_dice_coefficient(s1_addr, s2_addr)
    if dice_addr < 0.1:
        return(0.0)
    dice1 = (0.5 * dice_name + 0.5 * dice_addr)
    s1_contact = get_ngrams(get_applicant_contact_text(df.ix[i1]))
    s2_contact = get_ngrams(get_applicant_contact_text(df.ix[i2]))
    dice_contact = compute_dice_coefficient(s1_contact, s2_contact)
    dice2 = (0.45 * dice_name + 0.45 * dice_addr + 0.1 * dice_contact)
    dice = max(dice1, dice2)
    return(dice)

def find_row_similars(df, i, p = 0.7):
    similars = [i1 for i1 in range(len(df)) if compute_applicant_match_p(df, i, i1) > p]
    return df.ix[similars]

applicant_ngrams = namedtuple('applicant_ngrams', ['zone', 'name', 'address', 'contact'])

def compute_match_p(ng1, ng2):
    if ng1.zone != ng2.zone:
        return(0.0)
    dice_name = compute_dice_coefficient(ng1.name, ng2.name)
    if dice_name < 0.1:
        return(0.0)
    dice_addr = compute_dice_coefficient(ng1.address, ng2.address)
    if dice_addr < 0.1:
        return(0.0)
    dice = (0.5 * dice_name + 0.5 * dice_addr)
    return(dice)

def precompute_ngrams(df_companies):
    zone = [(df_companies.ix[idx].countrycode, df_companies.ix[idx].state) for idx in df_companies.index]
    ngrams_name = [get_ngrams(get_applicant_name_text(df_companies.ix[idx])) for idx in df_companies.index]
    ngrams_addr = [get_ngrams(get_applicant_address_text(df_companies.ix[idx])) for idx in df_companies.index]
    ngrams_nt = [applicant_ngrams(zone[i], ngrams_name[i], ngrams_addr[i], None) for i, _ in enumerate(df_companies.index)]
    return(ngrams_nt)


def deduplicate_applicants(df, p = 0.7):
    print("Deduplicating applicants")
    sys.stdout.flush()
    start_time = time.time()
    print("Removing trivial duplicates")
    df_companies = df.drop_duplicates(columns_company_df).copy()
    deduplicated_index = [None for i in df_companies.index]
    print("Computing name and address ngrams")
    ngrams_nt = precompute_ngrams(df_companies)
    num_rows = len(df_companies.index)
    print_interval = max(1, int(math.floor(num_rows / 20)))
    print("Doing ngram/Dice based matching")
    for i, idx in enumerate(df_companies.index):
        if deduplicated_index[i] is not None:
            continue
        deduplicated_index[i] = idx
        similars = [i2 for i2 in range(i + 1, len(df_companies.index)) if deduplicated_index[i2] is None and compute_match_p(ngrams_nt[i], ngrams_nt[i2]) > p]
        for i2 in similars:
            deduplicated_index[i2] = idx
        #print('Processing: %s' % repr([i] + similars))
        if i % print_interval == 0:
            print("%d percent processed" % int(100.0 * i / num_rows))
            sys.stdout.flush()
    print("Wraping up")
    df_companies['companyid'] = [df_companies.ix[idx].applicantid for idx in deduplicated_index]
    df_companies_unique = df_companies.loc[list(set(deduplicated_index))].copy()
    df2 = df.merge(df_companies, on=columns_company_df, how='left')
    df2['applicantid'] = df2.applicantid_x
    df2['contact'] = [re.sub('[\s]+', ' ', str(c)) for c in df2.contact_x]
    df_contact = df2[['applicantid', 'companyid', 'contact']].drop_duplicates(['companyid', 'contact'])
    df_contact['contactid'] = df_contact.applicantid
    df_contact = df_contact[['contactid', 'contact', 'companyid']]
    df2 = df2.merge(df_contact, on=['companyid', 'contact'], how='left')
    df2 = df2[['applicantid', 'contactid', 'companyid']]
    df_companies_unique = df_companies_unique[['companyid'] + columns_company_df]
    print("Done. Took %f seconds" % (time.time() - start_time))
    return(df2, df_companies_unique, df_contact)
    

def main(argv):
    parser = argp.ArgumentParser(description=' '.join(["Download FDA PMN", "data files and create", "SQL database"]))

    parser.add_argument('-e', '--engine', type=str, default='sqlite:///fdadevices.db', required=False)

    # Parse command line arguments
    args = parser.parse_args(argv[1:])

    print("Using engine: %s" % args.engine)

    # Download data
    print("Downloading FDA records")
    start_time = time.time() 
    d = download_fda_data(fda_url_pmn_all)
    print("Done. Took %f seconds" % (time.time() - start_time))

    # Connect to database
    print("Connecting to database")
    start_time = time.time() 
    engine = sqla.create_engine(args.engine)
    print("Done. Took %f seconds" % (time.time() - start_time))

    print("Inserting records in database")
    start_time = time.time() 
    create_database(engine, d)
    print("Done. Took %f seconds" % (time.time() - start_time))


if __name__ == "__main__":
    main(sys.argv)
