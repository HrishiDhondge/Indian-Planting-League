import streamlit as st
from pyspark.sql import SparkSession
from scripts import java_support


# get java support 
java_support.setup_java()


@st.cache_resource
def load_data():
    first_df = spark.read.parquet("./data/total_innings.parquet")
    sec_df = spark.read.parquet("./data/total_batting.parquet")

    first_df.createOrReplaceTempView("first_table")
    sec_df.createOrReplaceTempView("second_table")
    return first_df, sec_df

def run_sql_query(sql_file, num):
    '''This runs the sql query from the sql file and returns the result as a df'''
    with open(sql_file, "r") as file:
        query = file.read()
    query = query.format(top_N=num)
    return spark.sql(query)


# Set page configuration
st.set_page_config(
    page_title="Indian Planting League",
    page_icon="🏏",
    layout="centered",
    initial_sidebar_state="auto"
)

# Custom CSS for styling
st.markdown("""
    <style>
        body {
            background-color: #f8f9fa;
        }
        .main {
            background-color: #ffffff;
            border-radius: 15px;
            padding: 2rem;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        .title {
            font-size: 3rem;
            color: #0d6efd;
            font-weight: bold;
        }
        .subtitle {
            font-size: 1.5rem;
            color: #6c757d;
        }
        .footer {
            font-size: 0.9rem;
            color: #adb5bd;
            margin-top: 2rem;
            text-align: center;
        }
        .button {
            background-color: #0d6efd;
            color: white;
            padding: 0.75rem 1.5rem;
            border-radius: 10px;
            font-weight: bold;
            text-decoration: none;
        }
    </style>
""", unsafe_allow_html=True)

icon_url_1 = "https://images.seeklogo.com/logo-png/53/1/tata-ipl-logo-png_seeklogo-531750.png"
col1, col2, col3 = st.columns([1, 3, 1])

with col1:
    st.image(icon_url_1, width=80)

with col2:
    st.markdown("<h2 style='text-align: center;'>Indian Planting League</h2>", unsafe_allow_html=True)



# Main container
with st.container():
    # st.markdown('<div class="main">', unsafe_allow_html=True)
    
    st.markdown('<div class="title">Best Team of Indian Planting League (IPL) 2025 🏏</div>', unsafe_allow_html=True)
    
    # Create a Spark session
    spark = SparkSession.builder.appName('nestedJSON').getOrCreate()
    first_df, sec_df = load_data()

    st.subheader('Most plants planted at each Batting Position by a Single Player')

    with st.expander("⚙️ Query Configuration", expanded=False):
        with st.form("top_N"):
            my_number = st.slider('🎯 Show top N batters at each position:', 1, 10, value=3)
            submit = st.form_submit_button('🚀 Run Query')

    if submit:
        result_df = run_sql_query("./scripts/sql/most_dot_balls_per_bat_position.sql", my_number)
        st.success("✅ Query executed successfully!")
        st.dataframe(result_df, use_container_width=True)
    else:
        result_df = run_sql_query("./scripts/sql/most_dot_balls_per_bat_position.sql", 1)
        st.dataframe(result_df, use_container_width=True)


    st.markdown('<div class="footer">More data and analyses coming soon.</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)
