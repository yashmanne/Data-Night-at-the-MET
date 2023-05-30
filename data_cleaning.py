import pandas as pd
import numpy as np

def load_data():
    # Define the URL of the CSV file
    csv_url = "https://media.githubusercontent.com/media/metmuseum/openaccess/master/MetObjects.csv"

    # Download the CSV file using pandas and create a Pandas DataFrame
    raw_df = pd.read_csv(csv_url,low_memory=False)
    
    use_cols = [
        'Object ID', # Cleaned
        'Object Name', 
        'Country', # Cleaned
        'Culture', # Cleaned 
        'Period', # Cleanish - picked the first, broader description, if multiple present ex.: Min dynasty, dude 1 => ming dynasty
        'Dynasty', # removed
        'Reign', # removed
        'Medium', # Cleanish - some confusion about whether to use `,` and `;` as a delimiter (sometimes used for additional detail)
                        # for the scope of project I just split it
        'Is Highlight', # Cleaned
        'Gallery Number', # Cleaned
        'Department', # Cleared
        'Artist Display Name', # Cleaned
        'Artist Gender', # Cleaned
        'Artist Role' #Cleaned
    ]
    df = raw_df.loc[:, use_cols]
    
    df_pandas = clean_data(df)
    
    artwork, artist, medium = extract_final_tables(df_pandas)
    
    return artwork, artist, medium
    
def extract_final_tables(df_pandas):
    # Medium Table
    df_exploded_medium = df_pandas.explode('Medium')
    df_medium_pd = (
        df_exploded_medium[['Medium']]
        .drop_duplicates()
        .reset_index(drop=True)
        .reset_index()
        .rename(columns={"index": "ID", "Medium": "Material"})
    )
    # Artist Table
    df_exploded_name = df_pandas.explode('Artist Display Name')
    df_exploded_gender = df_pandas.explode('Artist Gender')
    df_exploded_name['Gender'] = df_exploded_gender['Artist Gender']
    df_exploded_name.loc[(df_exploded_name['Gender'] != 'Female'), 'Gender'] = 'Male'
    df_exploded_name.loc[(df_exploded_name['Artist Display Name'].isnull()), 'Gender'] = None
    df_artist_pd = df_exploded_name[['Gender', 'Artist Display Name']]
    df_artist_pd = df_artist_pd.drop_duplicates(subset=['Artist Display Name'])
    df_artist_pd = df_artist_pd.dropna()
    df_artist_pd.loc[:,'ID'] = list(range(df_artist_pd.shape[0]))
    df_artist_pd.rename(columns = {'Artist Display Name': 'Name'}, inplace=True)

    # Create Table
    ex_df = df_pandas.explode('Artist Display Name')
    ex_df = ex_df[['Artist Display Name', 'Object ID']]
    ex_df = ex_df.dropna()
    df_creates_pd = pd.merge(ex_df, df_artist_pd, left_on='Artist Display Name', right_on = 'Name', how='left')
    df_creates_pd.drop(['Artist Display Name', 'Name', 'Gender'], axis=1, inplace=True)
    df_creates_pd = df_creates_pd.rename(columns={'ID': 'Artist ID'})
    # df_creates = spark.createDataFrame(df_creates_pd)

    # Made With Table
    ex_df1 = df_pandas.explode('Medium')
    ex_df1 = ex_df1[['Object ID', 'Medium']]
    ex_df1 = ex_df1.dropna()
    df_madewith_pd = pd.merge(ex_df1, df_medium_pd, left_on = 'Medium', right_on = 'Material')
    df_madewith_pd.drop(['Medium', 'Material'], axis=1, inplace=True)
    df_madewith_pd.rename(columns={'ID': 'Medium ID'}, inplace=True)

    # Foreign Keys
    artist_ids = df_creates_pd.groupby(by='Object ID').agg(list)
    medium_ids = df_madewith_pd.groupby(by='Object ID').agg(list)

    # Artwork Table
    t_df = df_pandas[['Object ID', 'Object Name', 'Gallery Number',
                      'Medium', 'Department', 'Culture', 'Period',
                      'Country', 'Is Highlight', 'Artist Display Name']]
    df_artwork_pd = t_df.merge(artist_ids, on='Object ID', how='left')
    df_artwork_pd = df_artwork_pd.merge(medium_ids, on ='Object ID', how='left')
    df_artwork_pd.drop(columns=['Artist Display Name', 'Medium'], inplace=True)
    df_artwork_pd = df_artwork_pd.rename(columns={'Medium ID': 'Medium IDs', 'Artist ID': 'Artist IDs'})

    # Final Dataframes
    # df_artwork = spark.createDataFrame(df_artwork_pd)
    # df_artist = spark.createDataFrame(df_artist_pd)
    # df_medium = spark.createDataFrame(df_medium_pd)

    return df_artwork_pd, df_artist_pd, df_medium_pd
    
# Define function to split by '|' and return unique countries
def get_unique_values(row, delim='|'):
    values = []
    if (row == 'nan') or pd.isna(row):
        return [None]
    for v in row.split(delim):
        if v.strip():
            values.append(v.strip())
            if v.strip() == 'nan':
                values.append(None)
        else: 
            values.append(None)
    # if len(list(values)) > 0:
    #     return list(values)
    return values

    
def clean_data(df):
    # df_pandas is clean data
    df_pandas = df.copy()

    # Artist Attributes
    df_pandas['Artist Role'] = df_pandas['Artist Role'].astype(str)
    df_pandas['Artist Role'] = df_pandas['Artist Role'].apply(get_unique_values)
    df_pandas['Artist Display Name'] = df_pandas['Artist Display Name'].astype(str)
    df_pandas['Artist Display Name'] = df_pandas['Artist Display Name'].apply(get_unique_values)
    df_pandas['Artist Gender'] = df_pandas['Artist Gender'].astype(str)
    df_pandas['Artist Gender'] = df_pandas['Artist Gender'].apply(get_unique_values)

    artist_roles = df_pandas['Artist Role'].values
    all_roles = set([item for sublist in artist_roles for item in sublist])
    artists = df_pandas['Artist Display Name'].values
    artist_genders = df_pandas['Artist Gender'].values

    # filter out 
    not_an_artist = set(
        {'Sitter', 
    #      'Modeler', # refers to the person creating it not a person modeling or posing
         'Person in Photograph', 
         'Subject', 
         'Subject of book', 
         'Dedicatee'
         'Couture Line', 
         'Design House', 
         'Department Store'
         'Correspondent',
    #      'Factory owner',
    #      'Factory director',
    #      'Founder', # person still has input     
        }
    )

    from tqdm.auto import tqdm, trange
    for i in trange(len(artists)):
        assert len(artists[i]) == len(artist_genders[i]), print(i)
        assert len(artists[i]) == len(artist_roles[i]), print(i)

    new_artists = []
    new_genders = []
    new_roles = []
    for i in trange(len(artist_roles)):
        temp_artists = []
        temp_genders = []
        temp_roles = []
        for j, role in enumerate(artist_roles[i]):
            if role not in not_an_artist:
                temp_artists.append(artists[i][j])
                temp_genders.append(artist_genders[i][j])
                temp_roles.append(artist_roles[i][j])
        if len(temp_roles) == 0:
            new_artists.append([None])
            new_genders.append([None])
            new_roles.append([None])
        else:
            new_artists.append(temp_artists)
            new_genders.append(temp_genders)
            new_roles.append(temp_roles)
    df_pandas['Artist Role'] = new_roles
    df_pandas['Artist Gender'] = new_genders
    df_pandas['Artist Display Name'] = new_artists

    # Gallery Number
    df_pandas['Gallery Number'].replace("in Great Hall", 1001, inplace=True)
    df_pandas['Gallery Number'].replace("Petrie Ct. Café", 1002, inplace=True)
    df_pandas['Gallery Number'].replace("on Fifth Avenue", 1003, inplace=True)
    df_pandas['Gallery Number'].replace("Watson Library", 1004, inplace=True)
    df_pandas['Gallery Number'] = df_pandas['Gallery Number'].astype(float)
    df_pandas['Gallery Number'] = df_pandas['Gallery Number'].astype("Int64")

    # Period
    # Join Period & Dynasty/Reign
    contains_dynasty = set(df['Dynasty'].dropna().index.values)
    contains_reign = set(df['Reign'].dropna().index.values)
    contains_period = set(df['Period'].dropna().index.values)
    df_pandas.loc[list(contains_reign - contains_period), 'Period'] = df_pandas.loc[list(contains_reign - contains_period), 'Reign']
    df_pandas.loc[list(contains_dynasty - contains_period), 'Period'] = df_pandas.loc[list(contains_dynasty - contains_period), 'Dynasty']
    df_pandas.drop(columns = ['Dynasty', 'Reign'])

    # Culture
    # regular expression pattern to remove elements within parentheses
    pattern_parenthesis = r'\((.*?)\)'
    cleaned_culture = df_pandas['Culture'].str.replace(pattern_parenthesis, '', regex=True).str.strip()
    add_culture_info = df_pandas['Culture'].dropna().str.findall(pattern_parenthesis) # ignore this for scope of project
    # Get only the first cuture ( more often than not, the secondary values are city-specific, which we don't want to worry about)
    one_culture = cleaned_culture.apply(get_unique_values, delim= ',').apply(lambda x: x[0])
    # remove filler words
    one_culture = one_culture.str.replace(r'\b(possibly|probably|Possibly|Probably)\b', '', regex=True).str.strip()
    one_culture = one_culture.str.replace(r'\b(peoples|people|People|Peoples)\b', '', regex=True).str.strip()
    one_culture = one_culture.str.replace('?', '', regex=False).str.strip()
    one_culture = one_culture.str.replace(r'\s+', ' ', regex=True).str.strip()
    one_culture = one_culture.str.split(";", expand=True)[0]
    one_culture = one_culture.str.split(":", expand=True)[0]
    other_replace = {
        "Afghanistani": 'Afghan',
        'Afghanistan': 'Afghan',
        'India': 'Indian',
        'Argentinean':'Argentinian',
        'China': 'Chinese',
        'Columbia': 'Columbia', 
        'Guatemala': 'Guatemalan',
        'Indonesia': 'Indonesian',
        'Iran': 'Iranian',
        'Korea' : 'Korean',
        'Malayan': 'Malaysian',
        'Mexico': 'Mexican',
        'Nepal': 'Nepalese',
        'Nigeria' : 'Nigerian',
        'Netherlandish' : 'Dutch',
        'Peru': 'Peruvian',
        'Philippines': 'Philippine',
        'Papua New Guinea': 'Papua New Guinean',
        'Sri Lanka': 'Sri Lankan',
        'Sumatra': 'Sumatran',
        'Thailand': 'Thai',
        'Tibet':'Tibetan',
        'Unknown fabric': 'Unknown',
        'blade' : 'Unknown',
        'hilt': 'Unknown',
        'Sumba Island': 'Sumba' 
    }
    one_culture = one_culture.replace(other_replace)
    df_pandas['Culture'] = one_culture

    # Country
    # regular expression pattern to remove elements within parentheses
    cleaned_country = df_pandas['Country'].str.replace(pattern_parenthesis, '', regex=True).str.strip()
    # Get only the first country ( more often than not, the secondary values are city-specific, which we don't want to worry about)
    one_country = cleaned_country.apply(get_unique_values, delim= '|').apply(lambda x: x[0])

    # remove filler words
    one_country = one_country.str.replace(r'\b(possibly|probably|Possibly|Probably)\b', '', regex=True).str.strip()
    one_country = one_country.str.replace(r'\b(present-day|Present-Day|Present-day|modern-day|perhaps)\b', '', regex=True).str.strip()
    one_country = one_country.str.replace('from the', '', regex=False).str.strip()

    one_country = one_country.str.replace('?', '', regex=False).str.strip()
    one_country = one_country.str.replace(r'\s+', ' ', regex=True).str.strip()
    one_country = one_country.str.split(";", expand=True)[0]
    one_country = one_country.str.split(":", expand=True)[0]
    other_replace_country = {
        "USA": "United States",
        "U.S.A.": "United States",
        "US": "United States",
        "United States of America": "United States",
        'UK':'United Kingdom',
        'The Netherlands':'Netherlands',
        'Unknown country': 'Unknown',
        'italy':'Italy', 
        'india': 'India', 
        'iran': 'Iran'
    }
    one_country = one_country.replace(other_replace_country)
    df_pandas['Country'] = one_country

    # Period
    cleaned_period = df_pandas['Period'].str.replace(pattern_parenthesis, '', regex=True).str.strip()
    # Get only the first period ( more often than not, the secondary values are too specific, which we don't want to worry about)
    one_period = cleaned_period.apply(get_unique_values, delim= ',').apply(lambda x: x[0])

    # remove filler words
    one_period = one_period.str.replace(r'\b(possibly|probably|Possibly|Probably)\b', '', regex=True).str.strip()
    # one_period = one_period.str.replace(r'\b(middle|late|early|)\b', '', regex=True).str.strip()
    one_period = one_period.str.replace('from the', '', regex=False).str.strip()

    one_period = one_period.str.replace('?', '', regex=False).str.strip()
    one_period = one_period.str.replace(' /', '/', regex=False).str.strip()
    one_period = one_period.str.replace('/ ', '/', regex=False).str.strip()
    one_period = one_period.str.replace(r'\s+', ' ', regex=True).str.strip()
    one_period = one_period.str.split(";", expand=True)[0]
    one_period = one_period.str.split(":", expand=True)[0]
    other_replace_period = {
        "Date being researched":"Unknown",
        "Dates being researched":"Unknown",
    #     "Cypro-Archaic I" : "Cypro-Archaic",
    #     "Cypro-Archaic II": "Cypro-Archaic",
    #     "Cypro-Archaic I–II": "Cypro-Archaic",
    #     "Cypro-Classical I":"Cypro-Classical",
    #     "Cypro-Classical II":"Cypro-Classical",
    #     "Cypro-Classical I or II": "Cypro-Classical",
    #     "Cypro-Geometric I":"Cypro-Geometric",
    #     "Cypro-Geometric II":"Cypro-Geometric",
    #     "Cypro-Geometric III":"Cypro-Geometric"
    }
    one_period = one_period.replace(other_replace_period)
    df_pandas['Period'] = one_period

    # Medium
    # regular expression pattern to remove elements within parentheses

    pattern_parenthesis = r'\((.*?)\)'
    cleaned_medium = df_pandas['Medium'].str.replace(pattern_parenthesis, '', regex=True).str.strip()
    one_medium = cleaned_medium.str.lower()

    # remove filler words
    one_medium = one_medium.str.replace(r'\b(possibly|probably|Possibly|Probably)\b', '', regex=True).str.strip()
    one_medium = one_medium.str.replace('from the', '', regex=False).str.strip()

    one_medium = one_medium.str.replace('?', '', regex=False).str.strip()
    one_medium = one_medium.str.replace(r'[a-z]-[a-z]\)', '', regex=True).str.strip()
    one_medium = one_medium.str.replace(r'[a-z],[a-z]\)', '', regex=True).str.strip()
    one_medium = one_medium.str.replace(r'[a-z], [a-z]\)', '', regex=True).str.strip()
    one_medium = one_medium.str.replace(r'[a-z]\)', '', regex=True).str.strip()
    one_medium = one_medium.str.replace(r'[a-z]:', '', regex=True).str.strip()
    one_medium = one_medium.str.replace(r'\d+\)', '', regex=True).str.strip()

    one_medium = one_medium.str.replace(';', ',', regex=False).str.strip()

    # one_medium = one_medium.str.replace('alloy of', '', regex=False).str.strip()
    # one_medium = one_medium.str.replace(' and ', ',', regex=False).str.strip()

    one_medium = one_medium.str.replace('/ ', '/', regex=False).str.strip()
    one_medium = one_medium.str.replace('"', '', regex=False).str.strip()

    one_medium = one_medium.str.replace(r'\s+', ' ', regex=True).str.strip()
    # one_medium = one_medium.str.split(";", expand=True)[0]
    # one_medium = one_medium.str.split(":", expand=True)[0]
    other_replace_medium = {
        "[medium not available]":"Unknown",
        "[no medium available]":"Unknown",

    }
    one_medium = one_medium.replace(other_replace_medium)

    split_medium = one_medium.apply(get_unique_values, delim= ',')
    df_pandas['Medium'] = split_medium

    # change other values to None
    df_pandas = df_pandas.replace('nan', None)
    df_pandas = df_pandas.replace('Unknown', None)

    return df_pandas