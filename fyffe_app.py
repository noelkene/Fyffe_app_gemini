import streamlit as st
import base64
import vertexai
from vertexai.generative_models import GenerativeModel, Part, SafetySetting
from PIL import Image
from google.cloud import storage
from google.cloud import bigquery
import datetime
import io
import re

# --- Initialize services ---
vertexai.init(project="platinum-banner-303105", location="us-central1")
model = GenerativeModel("gemini-1.5-pro-002")
storage_client = storage.Client()
bq_client = bigquery.Client()

# --- Configuration ---
BUCKET_NAME = "fyffe_image_bucket"
DATASET_ID = "fyffe_dataset"
TABLE_ID = "banana_detections"

# --- Streamlit App ---
st.title("Banana Defect Detection")

# Initialize session state for unique IDs
if "count" not in st.session_state:
    st.session_state.count = 0

# Upload or Capture Image
uploaded_file = st.file_uploader("Upload an image", type=["jpg", "jpeg", "png"])
if st.button("Take Photo"):
    picture = st.camera_input("Take a picture")
    if picture:
        uploaded_file = picture

# Process Image
if uploaded_file is not None:
    # Display the uploaded image
    image = Image.open(uploaded_file)
    st.image(image, caption="Uploaded Image", use_column_width=True)

    # --- Store image in GCS ---
    try:
        # Generate a unique filename using session state count
        st.session_state.count += 1
        filename = f"{st.session_state.count}_{uploaded_file.name}"

        # Reset file pointer to the beginning
        uploaded_file.seek(0)

        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(filename)
        blob.upload_from_file(uploaded_file)
        image_uri = f"gs://{BUCKET_NAME}/{filename}"
        st.success(f"Image uploaded to GCS: {image_uri}")

        # Convert image to base64
        buffered = io.BytesIO()
        image.save(buffered, format="PNG")
        img_str = base64.b64encode(buffered.getvalue()).decode()
        image1 = Part.from_data(mime_type="image/png", data=img_str)

        # --- Vertex AI Configuration ---
        generation_config = {
            "max_output_tokens": 8192,
            "temperature": 1,
            "top_p": 0.95,
        }

        safety_settings = [
            SafetySetting(
                category=SafetySetting.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
                threshold=SafetySetting.HarmBlockThreshold.OFF
            ),
            SafetySetting(
                category=SafetySetting.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                threshold=SafetySetting.HarmBlockThreshold.OFF
            ),
            SafetySetting(
                category=SafetySetting.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
                threshold=SafetySetting.HarmBlockThreshold.OFF
            ),
            SafetySetting(
                category=SafetySetting.HarmCategory.HARM_CATEGORY_HARASSMENT,
                threshold=SafetySetting.HarmBlockThreshold.OFF
            ),
        ]
        pattern = r"Number of Bunches:\s*(\d+)\s+Bananas per Bunch:\s*(\d+)\s+Defect Type:\s*([a-zA-Z\s\/,]+)\s+Level of Defect:\s*([\w\s\d\-]+)\s+Additional Description:\s*(.+?)(?:\n|$)"

        # Generate response from Gemini
        responses = model.generate_content(
            [image1, """Describe in detail any defects with these bananas. For each image outline the following parameters with exactly these headlines
            Number of Bunches, Minimum bananas per Bunch, Defect Type, Level of Defect, Additional Description. Separate Bunches with a "Bunch X" label with X being a stepped integer.
If the images do not contain bananas respond “There are no bananas in this image”
Keep answers short and succinct.
Only output integers for Number of Bunches and Minimum bananas per Bunch
For Number of Bunches - label the Bunch with a number based on the number of bunches in the image starting from 1.
"""],
            generation_config=generation_config,
            safety_settings=safety_settings,
            stream=True,
        )

        # --- Store results in BigQuery ---
        response_text = ""
        for response in responses:
            response_text += response.text

        st.write(response_text)  # Display the full response
        print(response_text)

        # --- Clean up the response text ---
        cleaned_text = re.sub(r"\*\*|\*", "", response_text)  # Remove asterisks
        cleaned_text = re.sub(r"[^\S\n]+", " ", cleaned_text)  # This keeps \n but replaces other whitespace

        # Improved regular expression pattern

        # Split the response text by each bunch description
        bunches = re.split(r"Bunch \d+", cleaned_text)
        print ("bunches are equal to", bunches)
    
        rows_to_insert = []
        bunch_number = ""
        bananas_per_bunch = ""
        defect_type = ""
        defect_level = ""
        additional_description = ""
        for bunch in bunches:
#            match = re.search(pattern, bunch, re.DOTALL)
#            print ("match is equal to :", match)
#            if match:
#                bunch_number, bananas_per_bunch, defect_type, defect_level, additional_description = match.groups()
            for line in bunch.split('\n'):
                print("line is :",line)
                if line.startswith(" Number of Bunches: "):
                    bunch_number = line.replace(" Number of Bunches: ", "").strip()
                    print(bunch_number)
                elif line.startswith(" Minimum bananas per Bunch: "):
                    min_bananas_per_bunch = line.replace(" Minimum bananas per Bunch: ", "").strip()
                    print(min_bananas_per_bunch)
                elif line.startswith(" Defect Type: "):
                    defect_type = line.replace(" Defect Type: ", "").strip()
                    print(defect_type)
                elif line.startswith(" Level of Defect: "):
                    defect_level = line.replace(" Level of Defect: ", "").strip()
                    print(defect_level)
                elif line.startswith(" Additional Description: "):
                    additional_description = line.replace(" Additional Description: ", "").strip()
                    print(additional_description)

                # Ensure bunch_number and bananas_per_bunch are not empty before conversion
            if bunch_number and min_bananas_per_bunch:
                rows_to_insert.append(
                    {
                        "image_uri": image_uri,
                        "bunch_number": int(bunch_number),  # Only convert if it's not empty
                        "bananas_per_bunch": int(min_bananas_per_bunch),
                        "defect_type": defect_type.strip(),
                        "defect_level": defect_level.strip(),
                        "additional_description": additional_description.strip(),
                        "timestamp": datetime.datetime.now(),
                    }
                )
            else:
                st.warning("Bunch number or bananas per bunch are missing in the response.")

        # Insert data into BigQuery
        print(rows_to_insert)
        if rows_to_insert:
            table_ref = bq_client.dataset(DATASET_ID).table(TABLE_ID)
            table = bq_client.get_table(table_ref)
            errors = bq_client.insert_rows(table, rows_to_insert)
            if errors == []:
                st.success("Data inserted into BigQuery")
            else:
                st.error(f"Error inserting data into BigQuery: {errors}")
        else:
            st.warning("No defects found in the image.")

    except Exception as e:
        st.error(f"An error occurred: {e}")
