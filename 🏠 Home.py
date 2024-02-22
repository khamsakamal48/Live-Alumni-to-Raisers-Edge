import streamlit as st

########################################################################################################################
# Streamlit Defaults
########################################################################################################################
st.set_page_config(
    page_title='Live Alumni to Raisers Edge',
    page_icon=':arrows_counterclockwise:',
    layout="wide"
)

hide_streamlit_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

# Add a title and intro text
st.title('Live Alumni Data synchronizer')
st.text('This is a web app to perform data processing and syncing between Live Alumni and Raisers Edge.')
