import streamlit as st
import json
import certifi
import urllib3
from snowflake.snowpark.context import get_active_session

session = get_active_session()

# Initialize session state for page navigation if not already done
if 'page' not in st.session_state:
    st.session_state.page = 'Main Page'

# Function to change page
def change_page(page_name):
    st.session_state.page = page_name

# Define a generic footer function to be used across all pages
def footer():
    st.markdown("---")
    st.markdown("Privacy Policy | Terms of Service | Contact Us")

# Main Page with Use Cases
def main_page():
    st.header("Main Page")
    use_cases = [
        ("Identifying Fraudulent Claims and Billing Practices", "Fraud Detection Page"),
        ("Streamlining Customer Onboarding", "Onboarding Page"),
        ("AI-Powered Virtual Assistants for Health Information", "Virtual Assistant Page")
    ]
    for use_case, page_name in use_cases:
        st.subheader(f"{use_case}")
        st.write("Overview text")
        if st.button(f"Start {use_case.split()[0]}", key=page_name):
            change_page(page_name)

# Fraud Detection Page
def fraud_detection_page():
    st.header("Fraud Detection Page")
    st.text_area("Paste Test Dataset Here", key="paste_data", height=200)
    st.date_input("Date Range", key="date_range")
    st.selectbox("Fraud Detection Algorithm", ["Algorithm A", "Algorithm B", "Algorithm C"], key="algorithm")
    if st.button("Show Visualization", key="show_visualization"):
        st.line_chart([10, 20, 30, 40, 50])  # Placeholder for real visualization
    st.table({"Claim ID": ["C123", "C456"], "Status": ["Suspicious", "Review Needed"]})  # Example table
    st.download_button("Download Report", "Sample Report Content", key="download_report")

def onboarding_page():
    
    st.header("Resume Information Extraction")
    
    relevant_information = '''
    1. Extract the name of the individual mentioned in the resume.
    2. Identify the contact information (phone number, email address) of the individual.
    3. Extract the educational background, including schools attended and degrees obtained.
    4. Identify the work experience section and extract details such as job titles, company names, and durations of employment.
    5. Extract skills, tools, and technologies mentioned in the resume.
    6. Identify project descriptions and extract relevant information such as project durations and technologies used.
    7. Extract any additional information relevant to KYC requirements, such as certifications, languages spoken, or professional affiliations.
    '''
    resume_text = st.text_area("Paste Your Resume Here", value="", height=200)
    submit_button = st.button("Extract Information")
    resume_text = f'From {resume_text} extract {relevant_information}'
    resume_text = resume_text.replace('\n', '')
    #st.write(resume_text)
    if submit_button and resume_text:
        resume_prompt = {
            "prompt_text": resume_text
        }
        #st.write(resume_prompt)
        query = f"SELECT LAMBDA_HUGGINGFACE_CALL('{json.dumps(resume_prompt)}')"      
        try:
            result = session.sql(query).collect()
            if result:
                raw_response = json.loads(result[0][0])
            else:
                raise Exception(f'No response recieved from function calling lambda. Error is {e} and resume_prompt is {resume_prompt}')
            # Call the API and process the response
        except Exception as e:
            st.error(query)
            st.error(e)
            raise Exception(f'Failed to make api call, error:{e}')
        try:
            if raw_response:
                # Extract the first element from the list, which is a string that looks like a JSON list
                json_string = raw_response[0]
                
                # Find the position where the JSON structure ends and text begins
                start_index = json_string.find('}\n\n') + len('}\n\n')
                
                # Find the last closing brace to avoid capturing it
                end_index = json_string.rfind('}')
                
                # Extract the desired text between these indices
                extracted_text = json_string[start_index:end_index].strip()

                #st.write(f'extracted_text is {extracted_text}')

                # Start from the first occurrence of "\n\n" in the extracted_text
                start_index = extracted_text.find('}') + len('}')
                # Extract everything after "\n\n"
                final_text = extracted_text[start_index:].strip()
                final_text = final_text.lstrip('\n').strip()
                st.write("Final extracted content:")
                st.write(final_text)
            else:
                st.error("Failed to extract information. Please try again.")
        except Exception as e:
            st.error(f'raw_response is {raw_response}')
            st.error(f"Failed to process data from the server. Error: {e}")


def virtual_assistant_page():
    st.header("Virtual Assistant Page")
    user_prompt = st.text_input("Enter your health concern:", key="user_prompt")
    collection_name = st.text_input("Specify the collection to use:", key="collection_name")

    if st.button("Submit Query"):
        if user_prompt and collection_name:
            payload = json.dumps({
                "prompt_text": user_prompt,
                "collection_name": collection_name
            })
            try:
                query = "SELECT LAMBDA_EMBEDDINGS_CALL(:1)"
                result = session.sql(query, [payload]).collect()
                if result:
                    raw_response = result[0][0]
                    if isinstance(raw_response, str):
                        # Find the index of the prompt text in the response
                        index = raw_response.find(user_prompt)
                        if index != -1:
                            # Extract everything after the prompt text
                            processed_output = raw_response[index + len(user_prompt):]
                            st.write("Response to your query:")
                            st.write(processed_output)
                        else:
                            st.error("Prompt text not found in the response.")
                    else:
                        st.error("Received data is not in expected string format.")
                else:
                    st.error("No response received from the function.")
            except Exception as e:
                st.error(f"Failed to fetch data from the server. Error: {e}")

# Navigation function
def navigate():
    # Use the session state to determine which page to display
    if st.session_state.page == 'Main Page':
        main_page()
    elif st.session_state.page == 'Fraud Detection Page':
        fraud_detection_page()
    elif st.session_state.page == 'Onboarding Page':
        onboarding_page()
    elif st.session_state.page == 'Virtual Assistant Page':
        virtual_assistant_page()
    footer()

def main():
    st.sidebar.title("Navigation")
    # Add a button in the sidebar to go back to the main page
    if st.sidebar.button("Back to Main Page"):
        change_page('Main Page')
    navigate()

if __name__ == "__main__":
    main()
