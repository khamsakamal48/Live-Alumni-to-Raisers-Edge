import streamlit as st
import pandas as pd
import os
import re
import psycopg2
import subprocess
import sys
import zipfile
import shutil

from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from io import BytesIO


########################################################################################################################
#                                                   Load Functions                                                     #
########################################################################################################################

def get_env_variables():
    return {
        'DB_IP': os.getenv('DB_IP'),
        'DB_USER': os.getenv('DB_USER'),
        'DB_PASS': quote_plus(os.getenv('DB_PASS')),
        'DB_NAME': os.getenv('DB_NAME')
    }


def upload_data():
    st.header('Upload Data', divider='blue')

    st.subheader('Ensure that the following files are uploaded with the file name as mentioned below')

    df = load_data('Files/Import Files.csv')
    st.dataframe(df, hide_index=True, use_container_width=True)

    st.divider()

    files = st.file_uploader(
        label='Select files to upload',
        type='csv',
        accept_multiple_files=True,
        help='Upload files from Live Alumni and Raisers Edge'
    )

    return files


# @st.cache_data
def load_data(csv_file):
    df = pd.read_csv(csv_file)
    return df


def load_to_db(files):
    if files and st.button('**Upload Data**', use_container_width=True, type='primary'):

        st.info('Please stay on the page. Going back or navigating elsewhere would reset the request. Don\'t click any '
                'button as well.')

        initialize_db()

        for each_file in files:
            df = pd.read_csv(each_file, encoding='latin1', low_memory=False)

            file_name = re.sub(
                '[^a-zA-Z _]', '', each_file.name
            ).replace('csv', '').strip().title().replace(' ', '_')

            df.to_sql(
                name=file_name,
                con=connect_to_db(),
                if_exists='replace',
                index=False
            )

        # Upload Country Mapping
        country_mapping = load_data('Files/Country Mapping.csv')

        country_mapping.to_sql(
            name='Country_Mapping',
            con=connect_to_db(),
            if_exists='replace',
            index=False
        )

        st.success('Data Uploaded!', icon='✅')
        st.info('Kindly proceed to the Process..')
        available_options.append('Process')


def initialize_db():
    # Create a connection object
    conn = psycopg2.connect(
        dbname=get_env_variables().get('DB_USER'),
        user=get_env_variables().get('DB_USER'),
        host=get_env_variables().get('DB_IP'),
        password=get_env_variables().get('DB_PASS')
    )

    # Set the isolation level for the connection
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    # Create a cursor object
    cur = conn.cursor()

    # Use the psycopg2.sql module to create the query
    query = sql.SQL(
        f"DROP DATABASE IF EXISTS {os.getenv('DB_NAME')};"
    )

    # Execute the query
    cur.execute(query)

    query = sql.SQL(
        f"CREATE DATABASE {os.getenv('DB_NAME')};"
    )

    # Execute the query
    cur.execute(query)

    # Close the connection
    conn.close()


def connect_to_db():
    user = get_env_variables().get('DB_USER')
    password = get_env_variables().get('DB_PASS')
    db_ip = get_env_variables().get('DB_IP')
    db = get_env_variables().get('DB_NAME')

    return create_engine(f'postgresql+psycopg2://{user}:{password}@{db_ip}:5432/{db}', echo=False)


# Function that runs the python script
def run_script():
    result = subprocess.run([sys.executable, 'Processing.py'], capture_output=True, text=True)
    return result.stdout


def create_download_link():
    # Create a BytesIO buffer
    b = BytesIO()

    with zipfile.ZipFile(b, 'w') as zip_files:
        # Specify the directory of CSV files
        directory = 'Final'
        for filename in os.listdir(directory):
            if filename.endswith('.csv'):
                zip_files.write(os.path.join(directory, filename))

    b.seek(0)

    return b


########################################################################################################################
#                                                  Streamlit Defaults                                                  #
########################################################################################################################
st.set_page_config(
    page_title='Live Alumni to Raisers Edge',
    page_icon=':arrows_counterclockwise:',
    layout="wide")

hide_streamlit_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

# Add a title and intro text
st.title('Process Data from Live Alumni to upload in Raisers Edge')

st.divider()

########################################################################################################################
#                                                    SIDEBAR                                                           #
########################################################################################################################

with st.sidebar:
    available_options = ['1️⃣ Load Data', '2️⃣ Process Data', '3️⃣ Download Data']
    task = st.radio(
        label='What do you want to do?',
        captions=['Upload data from Live Alumni and RE', 'Compare data to identify new updates',
                  'Download the updates'],
        options=available_options
    )

########################################################################################################################
#                                                  1 - Load Data                                                       #
########################################################################################################################
uploaded = False

if task == available_options[0]:
    # Upload files
    uploaded_files = upload_data()

    if uploaded_files:
        uploaded_file_names = []
        for file in uploaded_files:
            uploaded_file_names.append(file.name)

        mandatory_files = ['Live Alumni.csv', 'Custom Fields.csv', 'Phone List.csv', 'Org Relationships.csv',
                           'Org Relationship Attributes.csv', 'Addresses.csv']

        # Check if all mandatory files are present
        if set(mandatory_files).issubset(set(uploaded_file_names)):
            st.success("All mandatory files are present.")

            # Connect DB
            client = connect_to_db()

            # Load to DB
            load_to_db(uploaded_files)

            uploaded = True

        else:
            missing_files = set(mandatory_files) - set(uploaded_file_names)
            st.error(f"The following mandatory files are missing: {', '.join(missing_files)}")

########################################################################################################################
#                                                 2 - Process Data                                                     #
########################################################################################################################
processed = False

if task == available_options[1]:
    st.header('Process Data', divider='blue')

    if uploaded is True:
        st.subheader('')

        if st.button(label='Process Data', type='primary', use_container_width=True):
            # Delete Previous files
            shutil.rmtree('Final')
            os.mkdir('Final')

            output = run_script()

            with st.container(height=600):
                st.code(output, language='shellSession')

            processed = True

    else:
        st.warning('Please Load data to process first!')

########################################################################################################################
#                                               3 - Download Data                                                      #
########################################################################################################################
if task == available_options[2]:
    st.header('Download Processed Data', divider='blue')

    if processed:
        # Create a download button
        st.header('📥  Download Data to upload in Raisers Edge')
        st.subheader('')

        buffer = create_download_link()
        st.download_button(
            label='DOWNLOAD',
            data=buffer,
            file_name='Live_Alumni_Data_to_upload_in_Raisers_Edge.zip',
            mime='application/zip',
            type='primary',
            use_container_width=True
        )

    else:
        st.warning('Please Load and Process data first to download!')
