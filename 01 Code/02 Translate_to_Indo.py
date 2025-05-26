import pandas as pd
import google.generativeai as genai
import os
from tqdm import tqdm
import time
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure the Gemini API
GOOGLE_API_KEY = os.getenv('GEMINI_API_KEY')
if not GOOGLE_API_KEY:
    raise ValueError("Please set GEMINI_API_KEY in your .env file")
genai.configure(api_key=GOOGLE_API_KEY)

# Initialize the model
model = genai.GenerativeModel('gemini-1.5-flash-latest')

# Create output directory if it doesn't exist
OUTPUT_DIR = "02 Data/db_v4/indo_ver"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def wrap_text_in_xml(text):
    """Wrap text in XML tags"""
    if pd.isna(text):
        return text
    return f"<text>{str(text)}</text>"

def extract_text_from_xml(xml_string):
    """Extract text from XML string"""
    if pd.isna(xml_string):
        return xml_string
    try:
        root = ET.fromstring(xml_string)
        return root.text
    except:
        return xml_string

def translate_text(text):
    """Translate text from English to Indonesian using Gemini"""
    if pd.isna(text):
        return text
    
    try:
        prompt = f"Translate this English text to Indonesian. Only provide the translation, no explanations:\n{text}"
        response = model.generate_content(prompt)
        translation = response.text.strip()
        return translation
    except Exception as e:
        print(f"Error translating text: {e}")
        return text

def save_progress(df, temp_path, final=False):
    """Save progress to a temporary file and clean up if it's the final save"""
    try:
        df.to_excel(temp_path, index=False)
        if final:
            # If this is the final save, move the temp file to the final location
            final_path = temp_path.replace('_temp', '')
            os.rename(temp_path, final_path)
            print(f"Saved final translation to {final_path}")
    except Exception as e:
        print(f"Error saving progress: {e}")

def process_excel_file(input_file, output_file, columns_to_translate):
    """Process an Excel file and translate specified columns"""
    print(f"\nProcessing {input_file}...")
    
    try:
        # Read the Excel file
        df = pd.read_excel(f"02 Data/db_v4/{input_file}")
        print(f"Processing all {len(df)} rows...")
        
        # Create a copy for Indonesian translations
        df_indo = df.copy()
        
        # Create temporary file path
        temp_output_file = output_file.replace('.xlsx', '_temp.xlsx')
        temp_output_path = os.path.join(OUTPUT_DIR, temp_output_file)
        
        # Process each column that needs translation
        for column in columns_to_translate:
            if column in df.columns:
                print(f"\nTranslating column: {column}")
                
                # Get unique values and create translation mapping
                unique_values = df_indo[column].dropna().unique()
                print(f"Found {len(unique_values)} unique values out of {len(df_indo)} total rows in {column}")
                
                # Create a mapping dictionary for translations
                translation_map = {}
                
                # Process unique values
                tqdm.write(f"Translating unique values in {column}...")
                for unique_text in tqdm(unique_values):
                    try:
                        # Wrap in XML
                        wrapped_text = wrap_text_in_xml(unique_text)
                        
                        # Translate if not already in mapping
                        if unique_text not in translation_map:
                            translated = translate_text(wrapped_text)
                            translation_map[unique_text] = extract_text_from_xml(translated)
                            
                            # Save progress every 10 unique values
                            if len(translation_map) % 10 == 0:
                                # Create temporary dataframe with current translations
                                temp_df = df_indo.copy()
                                temp_df[column] = temp_df[column].map(translation_map).fillna(temp_df[column])
                                save_progress(temp_df, temp_output_path)
                            
                            time.sleep(0.5)  # Add delay between API calls
                            
                    except Exception as e:
                        print(f"Error processing unique value '{unique_text}' in column {column}: {e}")
                        continue
                
                # Apply translations to the entire column using the mapping
                df_indo[column] = df_indo[column].map(translation_map).fillna(df_indo[column])
                print(f"Translated {len(translation_map)} unique values in {column}")
        
        # Save the final translated file
        final_output_path = os.path.join(OUTPUT_DIR, output_file)
        df_indo.to_excel(final_output_path, index=False)
        print(f"Saved final translation file to {final_output_path}")
        
        # Clean up temporary file if it exists
        if os.path.exists(temp_output_path):
            os.remove(temp_output_path)
        
    except Exception as e:
        print(f"Error processing file {input_file}: {e}")
        # Try to save whatever progress we have
        try:
            error_file = output_file.replace('.xlsx', '_error.xlsx')
            error_path = os.path.join(OUTPUT_DIR, error_file)
            df_indo.to_excel(error_path, index=False)
            print(f"Saved partial progress to {error_path}")
        except:
            print("Could not save error progress")

def main():
    # Define the translation tasks for each file
    translation_tasks = {
        'df_abilities_data.xlsx': {
            'output': 'df_abilities_data_indo.xlsx',
            'columns': ['ability_name', 'ability_description']
        },
        'df_desc_data.xlsx': {
            'output': 'df_desc_data_indo.xlsx',
            'columns': ['description']
        },
        'df_full_onet_digital.xlsx': {
            'output': 'df_full_onet_digital_indo.xlsx',
            'columns': ['Occupation', 'explanation']
        },
        'df_job_zones_data.xlsx': {
            'output': 'df_job_zones_data_indo.xlsx',
            'columns': ['job_zone_title', 'job_zone_education', 'job_zone_related_exp', 
                       'job_zone_training', 'job_zone_examples']
        },
        'df_knowledge_data.xlsx': {
            'output': 'df_knowledge_data_indo.xlsx',
            'columns': ['knowledge_main', 'knowledge_description']
        },
        'df_related_data.xlsx': {
            'output': 'df_related_data_indo.xlsx',
            'columns': ['related_title']
        },
        'df_tasks_data_2.xlsx': {
            'output': 'df_tasks_data_2_indo.xlsx',
            'columns': ['task_main', 'task_category']
        },
        'df_tech_skills_data_2.xlsx': {
            'output': 'df_tech_skills_data_2_indo.xlsx',
            'columns': ['best_title_gpt_name_x', 'Technology_Skills']
        },
        'df_demand_data_3.xlsx': {
            'output': 'df_demand_data_3_indo.xlsx',
            'columns': ['best_title_gpt_name_x', 'explanation']
        }
    }
    
    print(f"Output files will be saved to: {OUTPUT_DIR}")
    print("Processing all rows in each file")
    
    # Process each file
    for input_file, config in translation_tasks.items():
        try:
            process_excel_file(input_file, config['output'], config['columns'])
        except Exception as e:
            print(f"Failed to process {input_file}: {e}")
            continue

if __name__ == "__main__":
    main() 