import streamlit as st
import pandas as pd
import numpy as np

st.set_page_config(
    page_title='Identify New Live Alumni Matches',
    page_icon=':memo:',
    layout="wide")

hide_streamlit_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

# Title
st.title('Identify New matches for Live Alumni Data')
st.divider()


########################################################################################################################
#                                                   FUNCTIONS                                                          #
########################################################################################################################
def upload_data():
    # st.header('')

    st.header('Data Upload', divider='blue')

    col1, col2 = st.columns(2)
    with col1:
        st.subheader('Ensure that the following files are uploaded with the file name as mentioned below,')
        st.caption('These are mandatory files to upload.')

        df = load_data('Files/Import Files_1.csv')
        st.dataframe(df, hide_index=True, use_container_width=True)

    with col2:
        st.subheader('You can additionally add the following file with the file name as mentioned below,')
        st.caption('These are the Live Alumni matches that you identified manually.')

        st.dataframe(pd.read_csv('Files/Matches.csv'), hide_index=True, use_container_width=True)

        template = pd.read_csv('Files/Matches.csv')
        st.download_button('Download Matches Template', data=template.to_csv(index=False).encode('utf-8'),
                           file_name='Matches.csv',
                           mime='text/csv', use_container_width=True)

    st.divider()

    files = st.file_uploader(
        label='Select files to upload',
        type='csv',
        accept_multiple_files=True,
        help='Upload files from Live Alumni and Raisers Edge'
    )

    return files


def clean_linkedin(url):
    if url.endswith('/'):
        url = url[:-1]

    return url.replace('https://www.', '').replace('http://www.', '').replace('www.', '')


def format_import_id(import_id):
    return str(import_id)[0:5] + '-' + str(import_id)[5:8] + '-' + str(import_id)[-10:]


@st.cache_data
def load_data(csv_file):
    df = pd.read_csv(csv_file)
    return df


########################################################################################################################
#                                                 PAGE CONTENT                                                         #
########################################################################################################################
uploaded_file = upload_data()

if uploaded_file:
    # Save the uploaded files and check if all mandatory files are present
    uploaded_file_names = []
    for file in uploaded_file:
        uploaded_file_names.append(file.name)

    mandatory_files = ['Live Alumni.csv', 'Custom Fields.csv', 'Phone List.csv']

    # Check if all mandatory files are present
    if set(mandatory_files).issubset(set(uploaded_file_names)):
        st.success("All mandatory files are present.")

        if st.button(label='Process Data', type='primary', use_container_width=True):
            for file in uploaded_file:
                if file.name == 'Live Alumni.csv':
                    live_alumni = pd.read_csv(file, low_memory=False)

                elif file.name == 'Custom Fields.csv':
                    custom_fields = pd.read_csv(file, encoding='latin1', low_memory=False)

                elif file.name == 'Phone List.csv':
                    phones = pd.read_csv(file, encoding='latin1', low_memory=False)

                if file.name == 'Matches.csv':
                    manual = pd.read_csv(file, low_memory=False)

                else:
                    manual = pd.DataFrame()

            custom_fields_1 = custom_fields[custom_fields['CAttrCat'] == 'Live Alumni ID'][
                ['ConsID', 'CAttrDesc']].copy()

            live_alumni_1 = live_alumni.melt(
                id_vars='personid',
                value_name='ConsID',
                var_name='Type',
                value_vars=['Person Constituent ID', 'Person Level 1 Constituent ID', 'Person Level 2 Constituent ID']
            ).drop_duplicates(subset=['personid']).dropna().drop(columns='Type').sort_values(by=['ConsID'])

            new_matches_1 = live_alumni_1[
                ~(live_alumni_1['ConsID'].astype(int).isin(custom_fields_1['ConsID'].astype(int))) &
                ~(live_alumni_1['personid'].astype(int).isin(custom_fields_1['CAttrDesc'].astype(int)))
                ]

            phones = phones[
                (phones['PhoneType'].str.lower().str.contains('linkedin')) &
                (phones['PhoneIsInactive'] is False)
                ][['ConsID', 'PhoneNum']]

            phones['PhoneNum'] = phones['PhoneNum'].apply(lambda x: clean_linkedin(x))

            live_alumni_2 = live_alumni[['personid', 'Person URL']].drop_duplicates().copy()

            live_alumni_2['Person URL'] = live_alumni_2['Person URL'].apply(lambda x: clean_linkedin(x))

            new_matches_2 = live_alumni_2[
                (live_alumni_2['Person URL'].isin(phones['PhoneNum'])) &
                ~(live_alumni_2['personid'].astype(int).isin(custom_fields_1['CAttrDesc'].astype(int)))
                ].copy()

            new_matches_2 = new_matches_2.merge(phones, left_on='Person URL', right_on='PhoneNum', how='left')[
                ['personid', 'ConsID']]

            new_matches = pd.concat([new_matches_1, new_matches_2, manual], axis=0, ignore_index=True)

            new_matches.drop_duplicates(inplace=True)

            max_id = custom_fields['CAttrImpID'].replace('[^0-9]', '', regex=True).astype(int).sort_values(
                ascending=False)[0] + 9999999999

            new_matches_data = pd.DataFrame(data={
                'CAttrImpID': np.arange(max_id, max_id + new_matches.shape[0]),
                'CAttrCat': 'Live Alumni ID',
                'CAttrCom': np.NaN,
                'ConsID': new_matches['ConsID'].astype(int).values,
                'CAttrDate': pd.to_datetime('today').strftime('%d-%b-%Y'),
                'CAttrDesc': new_matches['personid'].values
            })

            new_matches_data['CAttrImpID'] = new_matches_data['CAttrImpID'].apply(lambda x: format_import_id(x))

            new_matches_data.to_csv('Final/New Live Alumni Matches.csv', quoting=1, lineterminator='\r\n',
                                    index=False)

            if new_matches_data.shape[0] > 0:
                st.download_button(
                    label='Download New Live Alumni Matches',
                    data=new_matches_data.to_csv(quoting=1, lineterminator='\r\n', index=False).encode('utf-8'),
                    file_name='New Live Alumni Matches.csv',
                    mime='text/csv',
                    use_container_width=True
                )

    else:
        missing_files = set(mandatory_files) - set(uploaded_file_names)
        st.error(f"The following mandatory files are missing: {', '.join(missing_files)}")
