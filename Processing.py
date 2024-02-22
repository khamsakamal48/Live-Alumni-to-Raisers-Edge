import pandas as pd
import numpy as np
import os
import random

from sqlalchemy import create_engine
from urllib.parse import quote_plus

from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from tabulate import tabulate


# Load Environment variables
def get_env_variables():
    return {
        'DB_IP': os.getenv('DB_IP'),
        'DB_USER': os.getenv('DB_USER'),
        'DB_PASS': quote_plus(os.getenv('DB_PASS')),
        'DB_NAME': os.getenv('DB_NAME')
    }


# Connect to Database
def connect_to_db():
    print('Connecting to database...')

    user = get_env_variables().get('DB_USER')
    password = get_env_variables().get('DB_PASS')
    db_ip = get_env_variables().get('DB_IP')
    db = get_env_variables().get('DB_NAME')

    return create_engine(f'postgresql+psycopg2://{user}:{password}@{db_ip}:5432/{db}', echo=False)


def sync_organisations():
    # Create Blank Dataframe
    org_df = pd.DataFrame()
    org_attributes_df = pd.DataFrame()

    # Get Data from Raisers Edge
    re_data = pd.read_sql_query(
        f"""
                        SELECT
                            *
                        FROM
                            "Org_Relationships"
                        WHERE
                            "ConsID" = {re_id};
                        """,
        con=client
    )

    # Get Data from Live Alumni
    la_data = pd.read_sql_query(
        f"""
        SELECT DISTINCT
            "Personal Industry Name",
            "Employment Company Name",
            "Employment Title",
            "Employment Start Year",
            "Employment Start Month",
            "Employment End Year",
            "Employment End Month",
            "Company Industry Name",
            "Employment Position Is Current",
            "Employment Position Is Primary",
            "Employment Title Is Senior",
            "Employment Salary Min",
            "Employment Salary Max",
            "Employment Seniority Level",
            "Company Record Standardized Name",
            "Company Record Historic Head Count",
            "Company Record Current Head Count",
            "Company Type Type",
            "Company Details Size",
            "Company Details Sector",
            "Company Details Website",
            "Person Headline"
        FROM
            "Live_Alumni"
        WHERE
            personid = {la_id};
        """,
        con=client
    )

    # Check if there's any data for the record in RE and LA
    if la_data.shape[0] and la_data['Employment Company Name'].values[0] is not None:

        # Export RE Organisations to list
        re_list = re_data['ORFullName'].drop_duplicates().dropna().tolist()

        # Identifying Start Date
        start_month = int(la_data['Employment Start Month'].values[0]) if la_data['Employment Start Month'].values[
                                                                              0] is not None else random.randint(1, 12)
        try:
            start_year = int(la_data['Employment Start Year'].values[0]) if la_data['Employment Start Year'].values[
                                                                                0] is not None else 0
        except ValueError:
            start_year = 0

        # Identifying End Date
        end_month = int(la_data['Employment End Month'].values[0]) if la_data['Employment End Month'].values[
                                                                          0] is not None else random.randint(1, 12)
        try:
            end_year = int(la_data['Employment End Year'].values[0]) if la_data['Employment End Year'].values[
                                                                            0] is not None else 0
        except ValueError:
            end_year = 0

        # Identifying Salary Range
        try:
            min_salary = int(la_data['Employment Salary Min'].values[0]) if la_data['Employment Salary Min'].values[
                                                                                0] is not None else 0
        except ValueError:
            min_salary = 0

        try:
            max_salary = int(la_data['Employment Salary Max'].values[0]) if la_data['Employment Salary Max'].values[
                                                                                0] is not None else 0
        except ValueError:
            max_salary = 0

        # Check if organisation is new/old
        match = process.extractOne(
            query=la_data['Employment Company Name'].values[0],
            choices=re_list,
            scorer=fuzz.ratio,
            score_cutoff=90
        )

        # Old
        if match is not None:
            # Update Existing Organisation
            org_name = match[0]

            import_id = re_data[re_data['ORFullName'] == org_name].head(n=1)['ORImpID'].values[0]

        # New
        else:
            # Add new Organisation
            import_id = format_import_id(int(max_org_import_id))

        re_new = pd.DataFrame(data={
            'ConsID': re_id,
            'ORImpID': import_id,
            'ORFromDate': np.NaN if start_year == 0 else pd.to_datetime(f'01-{start_month}-{start_year}',
                                                                        format='%d-%m-%Y').strftime('%d-%b-%Y'),
            'ORToDate': np.NaN if end_year == 0 else pd.to_datetime(f'01-{end_month}-{end_year}',
                                                                    format='%d-%m-%Y').strftime('%d-%b-%Y'),
            'ORIncome': np.NaN if min_salary == 0 or max_salary == 0 else f'${min_salary:,} - ${max_salary:,}',
            'ORIndustry': np.NaN if la_data['Company Industry Name'].values[0] is None else
            la_data['Company Industry Name'].values[0],
            'ORIsEmp': True,
            'ORIsPrimary': la_data['Employment Position Is Primary'].values[0],
            'ORFullName': la_data['Company Record Standardized Name'].values[0] if
            la_data['Company Record Standardized Name'].values[0] is not None else
            la_data['Employment Company Name'].values[
                0],
            'ORNotes': np.NaN if la_data['Person Headline'].values[0] is None else la_data['Person Headline'].values[0],
            'ORPos': np.NaN if la_data['Employment Title'].values[0] is None else la_data['Employment Title'].values[0],
            'ORProf': np.NaN if la_data['Company Industry Name'].values[0] is None else
            la_data['Company Industry Name'].values[0],
            'ORRecip': 'Employee',
            'ORRelat': 'Employer'
        }, index=[0])

        # Organisation Data
        org_df = pd.concat([org_df, re_new], axis=0, ignore_index=True)

        # Prepare Organisation Attributes
        attributes = sync_org_attributes(la_data, import_id)
        org_attributes_df = pd.concat([org_attributes_df, attributes], axis=0)

    return org_df, org_attributes_df


def sync_org_attributes(data, import_id):
    # print('\nWorking on Organisation Attributes...\n')

    # Get Sector
    sector_data = pd.DataFrame()

    sectors = data['Company Details Sector'].values[0]

    if sectors is not None:
        sectors = sectors.replace(', and ', ', ').replace(', ', ',').split(',')

        for sector in sectors:
            df = pd.DataFrame(data={
                'ORAttrORImpID': import_id,
                'ORAttrImpID': np.NaN,
                'ORAttrCat': 'Sector',
                'ORAttrDate': np.NaN,
                'ORAttrDesc': sector.title(),
                'ORAttrCom': 'Source: Live Alumni'
            }, index=[0])

            sector_data = pd.concat([sector_data, df], axis=0, ignore_index=True)

    # Employee Size
    emp_size_data = pd.DataFrame(data={
        'ORAttrORImpID': import_id,
        'ORAttrImpID': np.NaN,
        'ORAttrCat': 'Employee Size',
        'ORAttrDate': np.NaN,
        'ORAttrDesc': data['Company Details Size'].values[0],
        'ORAttrCom': 'Source: Live Alumni'
    }, index=[0])

    # Senior Position
    seniority_data = pd.DataFrame(data={
        'ORAttrORImpID': import_id,
        'ORAttrImpID': np.NaN,
        'ORAttrCat': 'Senior Position',
        'ORAttrDate': np.NaN,
        'ORAttrDesc': np.NaN if data['Employment Title Is Senior'].values[0] is None else
        data['Employment Title Is Senior'].values[0],
        'ORAttrCom': 'Source: Live Alumni'
    }, index=[0])

    # Company Type
    company_type = pd.DataFrame(data={
        'ORAttrORImpID': import_id,
        'ORAttrImpID': np.NaN,
        'ORAttrCat': 'Company Type',
        'ORAttrDate': np.NaN,
        'ORAttrDesc': np.NaN if data['Company Type Type'].values[0] is None else data['Company Type Type'].values[0],
        'ORAttrCom': 'Source: Live Alumni'
    }, index=[0])

    # Existing Attribute in RE
    existing_attributes = pd.read_sql_query(
        f"""
            SELECT
            *
        FROM
            "Org_Relationship_Attributes"
        WHERE
            "ORAttrORImpID" = '{import_id}';
        """,
        con=client
    )

    existing_attributes['ORAttrDate'] = np.NaN

    new_attributes = pd.concat([existing_attributes, seniority_data, sector_data, emp_size_data, company_type],
                               axis=0, ignore_index=True)

    # Dropping rows with no data
    new_attributes.dropna(subset=['ORAttrDesc'], inplace=True, ignore_index=True)

    # Dropping duplicate or existing values
    new_attributes.drop_duplicates(subset=['ORAttrCat', 'ORAttrDesc'], inplace=True, ignore_index=True)
    new_attributes = new_attributes[new_attributes['ORAttrImpID'].isnull()].reset_index(drop=True).copy()

    return new_attributes


def export_to_csv(df, filename):
    print(f'\nExporting data to {filename}...\n')
    df.to_csv(f'Final/{filename}', quoting=1, lineterminator='\r\n', index=False)


def format_import_id(import_id):
    return str(import_id)[0:5] + '-' + str(import_id)[5:8] + '-' + str(import_id)[-10:]


def format_org_attributes():
    # Adding Import IDs for Organisation Attributes
    org_attributes['ORAttrImpID'] = np.arange(int(max_org_attribute_imp_id),
                                              int(max_org_attribute_imp_id) + org_attributes.shape[0])

    org_attributes['ORAttrImpID'] = org_attributes['ORAttrImpID'].apply(lambda x: format_import_id(x))

    # Adding Dates
    org_attributes['ORAttrDate'] = pd.to_datetime('today').strftime('%d-%b-%Y')


def get_import_ids(id_name):
    match id_name:

        case 'max_org_import_id':
            max_id = pd.read_sql_query(
                """
                    SELECT
                        REPLACE("ORImpID", '-', '') AS id
                    FROM
                        "Org_Relationships"
                    ORDER BY
                        id DESC
                    LIMIT 1; 
                    """,
                con=client
            )['id'].astype(float).values[0] + 9999999999

        case 'max_org_attribute_imp_id':
            max_id = pd.read_sql_query(
                """
                    SELECT
                        REPLACE("ORAttrImpID", '-', '') AS id
                    FROM
                        "Org_Relationship_Attributes"
                    ORDER BY
                        id DESC
                    LIMIT 1;
                    """,
                con=client
            )['id'].astype(float).values[0] + 9999999999

        case 'max_address_imp_id':
            max_id = pd.read_sql_query(
                """
                    SELECT
                        REPLACE("AddrImpID", '-', '') AS id
                    FROM
                        "Addresses"
                    ORDER BY
                        id DESC
                    LIMIT 1;
                    """,
                con=client
            )['id'].astype(float).values[0] + 9999999999

        case 'max_phone_import_id':
            max_id = pd.read_sql_query(
                """
                    SELECT
                        REPLACE("PhoneImpID", '-', '') AS id
                    FROM
                        "Phone_List"
                    ORDER BY
                        id DESC
                    LIMIT 1;
                    """,
                con=client
            )['id'].astype(float).values[0] + 9999999999

        case 'max_attribute_import_id':
            max_id = pd.read_sql_query(
                """
                    SELECT
                        REPLACE("CAttrImpID", '-', '') AS id
                    FROM
                        "Custom_Fields"
                    ORDER BY
                        id DESC
                    LIMIT 1;
                    """,
                con=client
            )['id'].astype(float).values[0] + 9999999999

        case _:
            max_id = random.randint(1000000000, 9999999999)

    return int(max_id)


def sync_linkedin():
    # Get missing LinkedIn URLs in RE
    linkedin = pd.read_sql_query(
        """
        WITH la_ids as (
                SELECT
                    DISTINCT
                    CAST("ConsID" AS INT) AS re_id,
                    CAST("CAttrDesc" AS INT) AS la_id
                FROM
                    "Custom_Fields"
                WHERE
                    "CAttrCat" = 'Live Alumni ID'
            )
    
            SELECT
                    DISTINCT
                    REPLACE("Person URL", 'https://www.', '') AS phone,
                    personid AS la_id,
                    re_id
                FROM
                    "Live_Alumni" AS la
                    RIGHT JOIN la_ids ON la.personid = la_ids.la_id
                WHERE
                    REPLACE("Person URL", 'https://www.', '') NOT IN (
                        SELECT
                            CASE
                                WHEN RIGHT("PhoneNum",1) = '/'
                                    THEN LEFT(REPLACE(REPLACE(REPLACE("PhoneNum", 'https://www.', ''), 
                                    'http://www', ''), 'www.', ''),
                                            LENGTH(REPLACE(REPLACE(REPLACE("PhoneNum", 'https://www.', ''), 
                                            'http://www', ''), 'www.', '')) - 1)
                                ELSE
                                    REPLACE(REPLACE(REPLACE("PhoneNum", 'https://www.', ''), 
                                    'http://www', ''), 'www.', '')
                            END
                        FROM
                            "Phone_List"
                        WHERE
                            "PhoneType" LIKE 'LinkedIn%%'
                        );
        """,
        con=client
    )

    # Reformat to the way RE needs
    df = pd.DataFrame(data={
        'PhoneType': np.NaN,
        'PhoneImpID': np.NaN,
        'ConsID': linkedin['re_id'].values,
        'PhoneIsInactive': False,
        'PhoneIsPrimary': False,
        'PhoneComments': 'Captured from Live Alumni',
        'PhoneNum': linkedin['phone'].values
    })

    # Sorting the records
    df.sort_values(by='ConsID', ascending=True, inplace=True)

    # Identify the Phone Type for each phone
    phone_id = get_phone_id(df, 'linkedin')

    # Add the derived Phone Type to Dataframe
    df['PhoneType'] = phone_id

    return df


def get_phone_id(phone_df, phone_type):
    phone_type = phone_type.lower()

    # Check whether it's LinkedIn or email
    match phone_type:
        case 'linkedin':
            phone_type = 'LinkedIn'

        case 'email':
            phone_type = 'Email'

        case _:
            phone_type = None

    # Create a blank empty list of the new phone id
    phone_ids = []

    if phone_type is not None:

        # Deal with same constituent having multiple phones
        cons_id_list = phone_df['ConsID'].drop_duplicates().tolist()

        # Loop over each constituent ID
        for c_id in cons_id_list:
            # Create a dataframe of that constituent id
            df_1 = phone_df[phone_df['ConsID'] == c_id].reset_index(drop=True)
            new_id = 1

            # Loop over each row in the dataframe
            for i, r in df_1.iterrows():

                # Query database when we don't know the max id
                if i == 0:
                    new_id = pd.read_sql_query(
                        f"""
                        SELECT COALESCE(
                            (SELECT
                                CASE
                                    WHEN REGEXP_REPLACE("PhoneType", '[^0-9]+', '', 'g') = '' THEN 0
                                    ELSE CAST(REGEXP_REPLACE("PhoneType", '[^0-9]+', '', 'g') AS INT)
                                END + {i} AS phone_id
                            FROM
                                "Phone_List"
                            WHERE
                                "ConsID" = {c_id} AND
                                "PhoneType" LIKE '{phone_type}%%' AND
                                REGEXP_REPLACE("PhoneType", '[^0-9]+', '', 'g') != '' AND
                                CAST(REGEXP_REPLACE("PhoneType", '[^0-9]+', '', 'g') AS INT) < 100
                            ORDER BY
                                CAST(REGEXP_REPLACE("PhoneType", '[^0-9]+', '', 'g') AS INT) DESC
                            LIMIT 1),
                            1
                        ) AS phone_id;
                        """,
                        con=client
                    ).values[0][0]
                    phone_ids.append(f'{phone_type} {new_id}')

                # No need to query the DB since we already know the max id
                else:
                    phone_ids.append(f'{phone_type} {i + new_id}')

    return phone_ids


def sync_email():
    # Get data from Live Alumni
    la_emails = pd.read_sql_query(
        """
        WITH la_ids as (
            SELECT
                DISTINCT
                CAST("ConsID" AS INT) AS re_id,
                CAST("CAttrDesc" AS INT) AS la_id
            FROM
                "Custom_Fields"
            WHERE
                "CAttrCat" = 'Live Alumni ID'
        )

        SELECT
            DISTINCT
            LOWER("Contact Data Business Email") AS email_1,
            LOWER("Person Email") AS email_2,
            personid
        FROM
            "Live_Alumni" AS la
            RIGHT JOIN la_ids ON la.personid = la_ids.la_id
        WHERE
            "Contact Data Business Email" IS NOT NULL OR
            "Person Email" IS NOT NULL;
        """,
        con=client
    )

    # Get count of semicolons
    email_1 = la_emails['email_1'].str.replace(
        '[^;]', '', regex=True).replace(', ', '', regex=True).str.len().dropna().drop_duplicates().tolist()
    email_2 = la_emails['email_2'].str.replace(
        '[^;]', '', regex=True).replace(', ', '', regex=True).str.len().dropna().drop_duplicates().tolist()
    email_1.extend(email_2)
    count = max(email_1)

    # Creating Additional columns for the number of emails and filling with NaN
    for c in range(1,
                   int(count) + 2):  # Adding 1 since the count of values would exceed by 1 than the delimiter count,
        # another 1 for the range
        n = c + 2
        la_emails[f'email_{n}'] = np.NaN

    # Create an empty placeholder dataframe
    email_ = pd.DataFrame()

    # Get the column list of emails
    cols = [x for x in la_emails.columns.tolist() if x.startswith('email_')]

    # Split emails
    email_[[x for x in la_emails.columns.tolist() if x.startswith('email_')]] = la_emails[['email_1', 'email_2']].apply(
        lambda x: split_emails(*x, la_emails), axis=1, result_type='expand')

    # Add live alumni id to these dataframe
    email_['personid'] = la_emails['personid']

    # Replace the dataframe
    la_emails = email_.copy()
    del email_

    # Melting the dataframe
    la_emails = pd.melt(
        la_emails,
        id_vars=['personid'],
        value_vars=la_emails[cols],
        var_name='email_type',
        value_name='email'
    ).dropna().drop_duplicates().copy()

    # Converting email addresses to lower cases
    la_emails['email'] = la_emails['email'].str.lower()

    # Dropping iitb.ac.in email address
    la_emails = la_emails[~la_emails['email'].str.endswith('@iitb.ac.in')].reset_index(drop=True).copy()

    # Dropping email type column
    la_emails.drop(columns=['email_type'], inplace=True)

    # Get data from RE
    re_emails = pd.read_sql_query(
        """
        SELECT
            DISTINCT "PhoneNum" AS email
        FROM
            "Phone_List"
        WHERE
            "PhoneType" LIKE 'Email%%';
        """,
        con=client
    )

    # Get the missing email addresses not present in RE
    missing_emails = la_emails[~la_emails['email'].isin(re_emails['email'])]

    # Get RE IDs of the above missing email address
    missing_emails_with_id = pd.read_sql_query(
        f"""
        SELECT
            DISTINCT
            CAST("ConsID" AS INT) AS re_id,
            CAST("CAttrDesc" AS INT) AS personid
        FROM
            "Custom_Fields"
        WHERE
            "CAttrCat" = 'Live Alumni ID' AND
            CAST("CAttrDesc" AS INT) IN {tuple(missing_emails['personid'].tolist())};
        """,
        con=client
    )

    # Get records with missing email ID and live alumni linked in RE
    missing_emails = missing_emails.merge(missing_emails_with_id, on='personid', how='left')

    # Drop duplicates and non-matches values
    missing_emails = missing_emails.dropna().drop_duplicates().reset_index(drop=True).copy()

    # Reformat to the way RE needs
    df = pd.DataFrame(data={
        'PhoneType': np.NaN,
        'PhoneImpID': np.NaN,
        'ConsID': missing_emails['re_id'].values,
        'PhoneIsInactive': False,
        'PhoneIsPrimary': False,
        'PhoneComments': 'Captured from Live Alumni',
        'PhoneNum': missing_emails['email'].values
    })

    # Sorting the records
    df.sort_values(by='ConsID', ascending=True, inplace=True)

    # Identify the Phone Type for each phone
    phone_id = get_phone_id(df, 'linkedin')

    # Add the derived Phone Type to Dataframe
    df['PhoneType'] = phone_id

    # Verified Emails
    df_1 = pd.DataFrame(data={
        'CAttrImpID': np.NaN,
        'CAttrCat': 'Verified Email',
        'CAttrCom': 'Live Alumni',
        'ConsID': missing_emails['re_id'].values,
        'CAttrDate': np.NaN,
        'CAttrDesc': missing_emails['email'].values
    })

    # Sync Source
    df_2 = pd.DataFrame(data={
        'CAttrImpID': np.NaN,
        'CAttrCat': 'Sync source',
        'CAttrCom': missing_emails['email'].values,
        'ConsID': missing_emails['re_id'].values,
        'CAttrDate': np.NaN,
        'CAttrDesc': 'Live Alumni | Email'
    })

    return df, df_1, df_2


def split_emails(email1, email2, df):
    if email1 is None:
        email1 = []
    else:
        email1 = [email1]

    if email2 is None:
        email2 = []
    else:
        email2 = [email2]

    email1.extend(email2)

    email = [email.strip() for sublist in email1 for email in sublist.split("; ")]

    # Get the difference between emails in dataframe and the split emails
    diff = len([x for x in df.columns.tolist() if x.startswith('email_')]) - len(email)

    # If the difference is positive, append NaN values to email
    if diff > 0:
        email.extend([np.NaN] * diff)

    return email


def sync_address():
    # Get new addresses
    new_addresses = pd.read_sql_query(
        """
        WITH la_ids AS (
                SELECT
                    DISTINCT
                    CAST("ConsID" AS INT) AS re_id,
                    CAST("CAttrDesc" AS INT) AS la_id
                FROM
                    "Custom_Fields"
                WHERE
                    "CAttrCat" = 'Live Alumni ID'
            ),
            address_data AS (
                SELECT
                    DISTINCT
                    personid AS la_id,
                    re_id,
                    "Location City" AS la_city,
                    "Location State/Province" AS la_state,
                    CASE
                        WHEN "Country in Raisers Edge" IS NULL THEN "Location Country"
                        ELSE "Country in Raisers Edge"
                    END AS la_country,
                    "AddrCity" AS re_city,
                    CASE
                        WHEN "AddrCounty" IS NULL THEN "AddrState"
                        ELSE "AddrCounty"
                    END AS re_state,
                    "AddrCountry" AS re_country
                FROM
                    "Live_Alumni" AS la
                    JOIN la_ids ON la.personid = la_ids.la_id
                    JOIN "Country_Mapping" c ON la."Location Country" = c."Country in Live Alumni"
                    JOIN "Addresses" a ON a."ConsID" = re_id
                WHERE
                    a."PrefAddr" = TRUE
            )

        SELECT
            re_id,
            la_city AS city,
            la_state AS state,
            la_country AS country
        FROM
            address_data
        WHERE
            la_city != re_city OR
            la_state != re_state OR
            la_country != re_country;
        """,
        con=client
    )

    max_address_imp_id = get_import_ids('max_address_imp_id')

    # Create Address Dataframe
    df = pd.DataFrame(data={
        'AddrImpID': np.arange(max_address_imp_id, max_address_imp_id + new_addresses.shape[0]),
        'ConsID': new_addresses['re_id'].values,
        'AddrCity': new_addresses['city'].values,
        'AddrCounty': new_addresses['state'].values,
        'AddrState': new_addresses['state'].values,
        'AddrCountry': new_addresses['country'],
        'PrefAddr': True,
        'AddrType': 'LinkedIn'
    })

    #  Verified Locations
    df_1 = pd.DataFrame(data={
        'CAttrImpID': np.NaN,
        'CAttrCat': 'Verified Location',
        'CAttrCom': 'Live Alumni',
        'ConsID': new_addresses['re_id'].values,
        'CAttrDate': np.NaN,
        'CAttrDesc': new_addresses['city'].str.cat(new_addresses[['state', 'country']], sep=', ')
    })

    # Sync Source
    df_2 = pd.DataFrame(data={
        'CAttrImpID': np.NaN,
        'CAttrCat': 'Sync source',
        'CAttrCom': new_addresses['city'].str.cat(new_addresses[['state', 'country']], sep=', '),
        'ConsID': new_addresses['re_id'].values,
        'CAttrDate': np.NaN,
        'CAttrDesc': 'Live Alumni | Location'
    })

    return df, df_1, df_2


try:
    client = connect_to_db()

    ####################################################################################################################
    #                                                    1. MAPPING                                                    #
    ####################################################################################################################

    # Mapping RE ID with Live Alumni ID
    print('\nMapping RE ID with respective Live Alumni ID... \n')
    mapping = pd.read_sql_query(
        """
        SELECT
            "ConsID" AS re_id,
            "CAttrDesc" AS la_id
        FROM
            "Custom_Fields"
        WHERE
            "CAttrCat" = 'Live Alumni ID';
        """,
        con=client
    )

    re_ids_to_sync = mapping['re_id'].drop_duplicates().to_list()

    ####################################################################################################################
    #                                                2. Create Empty Dataframes                                        #
    ####################################################################################################################

    # 1. Organisation
    org = pd.DataFrame()
    org_attributes = pd.DataFrame()

    # # 2. Phone
    # phone = pd.DataFrame()
    # phone_attributes = pd.DataFrame()

    # Get Import IDs
    max_org_import_id = get_import_ids('max_org_import_id')
    max_org_attribute_imp_id = get_import_ids('max_org_attribute_imp_id')

    ####################################################################################################################
    #                                          3. Looping each record for comparison                                   #
    ####################################################################################################################

    # Looping through each RE ID
    for re_id in re_ids_to_sync:

        # Get corresponding Live Alumni ID(s)
        live_alumni_ids = mapping[mapping['re_id'] == re_id]['la_id'].drop_duplicates().to_list()

        print('Working on Organisations...\n')

        for la_id in live_alumni_ids:
            # print('_______________________________________________________________________\n')
            # print(f'Working on record with Raisers Edge ID: {re_id}')
            # print(f'The Live Alumni ID mapped with this record is: {la_id}')
            # print('_______________________________________________________________________\n')

            ############################################################################################################
            #                                           3.1 Organisations                                              #
            ############################################################################################################

            # Organisations
            org_data, org_data_attributes = sync_organisations()

            org = pd.concat([org, org_data], axis=0, ignore_index=True)
            org_attributes = pd.concat([org_attributes, org_data_attributes], axis=0, ignore_index=True)

            # Generate Import IDs
            max_org_import_id += 1
            max_org_attribute_imp_id += 1

    # Formatting the Organisation Attributes
    format_org_attributes()

    # Sync Source
    new_organisations = pd.DataFrame(data={
        'CAttrImpID': np.NaN,
        'CAttrCat': 'Sync Source',
        'CAttrCom': org['ORFullName'].values,
        'ConsID': org['ConsID'].values,
        'CAttrDate': np.NaN,
        'CAttrDesc': 'Live Alumni | Employment'
    })

    ####################################################################################################################
    #                                                  4. LinkedIn                                                     #
    ####################################################################################################################

    print('Working on LinkedIn URLs...\n')
    linkedin_data = sync_linkedin()

    ####################################################################################################################
    #                                                  5. Emails                                                       #
    ####################################################################################################################

    print('Working on Email addresses...\n')
    email_data, verified_email, new_emails = sync_email()

    # All Phones combined
    phone_data = pd.concat([linkedin_data, email_data], axis=0, ignore_index=True)

    # Phones Import ID
    max_phone_import_id = get_import_ids('max_phone_import_id')
    phone_data['PhoneImpID'] = np.arange(max_phone_import_id, max_phone_import_id + phone_data.shape[0])

    phone_data['PhoneImpID'] = phone_data['PhoneImpID'].apply(lambda x: format_import_id(x))

    ####################################################################################################################
    #                                                  6. Addresses                                                    #
    ####################################################################################################################

    print('Working on Addresses...\n')
    address, verified_address, new_address = sync_address()

    # Format Address Import ID
    address['AddrImpID'] = address['AddrImpID'].apply(lambda x: format_import_id(x))

    ####################################################################################################################
    #                                               7. Custom Fields                                                   #
    ####################################################################################################################

    # Format Attributes
    custom_fields = pd.concat([
        verified_email, new_emails, verified_address, new_address, new_organisations
    ], axis=0, ignore_index=True)

    # Format Attributes Import ID
    max_attribute_import_id = get_import_ids('max_attribute_import_id')
    custom_fields['CAttrImpID'] = np.arange(max_attribute_import_id, max_attribute_import_id + custom_fields.shape[0])

    custom_fields['CAttrImpID'] = custom_fields['CAttrImpID'].apply(lambda x: format_import_id(x))

    # Date
    custom_fields['CAttrDate'] = pd.to_datetime('today').strftime('%d-%b-%Y')

    # Ensuring Custom Field comments are less than 50 characters
    custom_fields['custom_fields'] = custom_fields['custom_fields'].str[:50]

    ####################################################################################################################
    #                                                 Final Data                                                       #
    ####################################################################################################################

    print('\nFinal Data of Organisations:\n')
    print(tabulate(org.fillna('').sample(n=10), headers='keys', tablefmt='pretty', showindex=False, missingval=''))

    print('\nFinal Data of Organisation Attributes:\n')
    print(tabulate(
        org_attributes.fillna('').sample(n=10), headers='keys', tablefmt='pretty', showindex=False, missingval=''))

    print('\nFinal Data of Phones:\n')
    print(tabulate(
        phone_data.fillna('').sample(n=10), headers='keys', tablefmt='pretty', showindex=False, missingval=''))

    print('\nFinal Data of Addresses:\n')
    print(tabulate(
        address.fillna('').sample(n=10), headers='keys', tablefmt='pretty', showindex=False, missingval=''))

    print('\nFinal Data of Custom Fields:\n')
    print(tabulate(
        custom_fields.fillna('').sample(n=10), headers='keys', tablefmt='pretty', showindex=False, missingval=''))

    ####################################################################################################################
    #                                              Exporting Data to CSV                                               #
    ####################################################################################################################
    export_to_csv(org, 'Organisations.csv')
    export_to_csv(org_attributes, 'Organisation Attributes.csv')
    export_to_csv(phone_data, 'Phones.csv')
    export_to_csv(address, 'Address.csv')
    export_to_csv(custom_fields, 'Custom_Fields.csv')

except Exception as e:
    print(e)
