# Import required packages
import pyreadstat
import pandas as pd
import os
from datetime import datetime
import numpy as np

# Global Variables for Easy Editing
SURVEY_TYPE = "Insert Data"
RESPONSE_TYPE = "Insert Data"
CLUB_INDEX = "Insert Data"
CLUB_NAME = "Insert Data"
CLUB_ADDRESS_CITY = "Insert Data"
CLUB_ADDRESS_STATE = "Insert Data"
CLUB_ADDRESS_ZIP = "Insert Data"
DUE_DATE = "Insert Data"  # ISO 8601 Date Format
CLUB_CATEGORY = "Insert Data"
CLUB_TYPE = 10
CLUB_ADDRESS_REGION = 10
TOTAL_MEMBER_COUNT = 10
TOTAL_SPOUSE_COUNT = 10
SURVEY_RESPONSE_MEMBER = None #will be updated dynamically
SURVEY_RESPONSE_SPOUSE = None #will be updated dynamically
TOTAL_RESPONSE_COUNT = None #will be updated dynamically

# Path Configuration
BASE_PATH = r"C:\\Insert\\Directory\\Folder\\Path"
MEMBER_FILE = "Insert SAV Path"
SPOUSE_FILE = "Insert SAV Path"
OUTPUT_FILE = "Insert Output File Path"
METADATA_FILE = "Insert Metadata File Path"

# Variables to Delete
UNNECESSARY_VARIABLES = [
    'Status', 'RecipientLastName', 'RecipientFirstName',
    'RecipientEmail', 'DistributionChannel', 'UserLanguage', 'ExternalReference'
]

def rename_variable(df, meta, old_name, new_name):
    """
    Rename a variable in the DataFrame and update metadata.
    """
    if old_name not in df.columns:
        print(f"Variable '{old_name}' not found. Skipping rename.")
        return df, meta

    print(f"Renaming variable '{old_name}' to '{new_name}'...")
    # Rename the column in the DataFrame
    df = df.rename(columns={old_name: new_name})

    # Update metadata
    meta.column_names = [new_name if name == old_name else name for name in meta.column_names]
    meta.column_labels = [new_name if label == old_name else label for label in meta.column_labels]
    if old_name in meta.variable_value_labels:
        meta.variable_value_labels[new_name] = meta.variable_value_labels.pop(old_name)
    if old_name in meta.variable_display_width:
        meta.variable_display_width[new_name] = meta.variable_display_width.pop(old_name)
    if old_name in meta.variable_measure:
        meta.variable_measure[new_name] = meta.variable_measure.pop(old_name)
    if old_name in meta.original_variable_types:
        meta.original_variable_types[new_name] = meta.original_variable_types.pop(old_name)
    if old_name in meta.variable_storage_width:
        meta.variable_storage_width[new_name] = meta.variable_storage_width.pop(old_name)

    print(f"Variable '{old_name}' successfully renamed to '{new_name}'.")
    return df, meta

def clean_sysmis_responses(df, start_index, end_index):
    columns_to_check = df.columns[start_index:end_index]
    print(f"Cleaning SYSMIS responses for columns: {list(columns_to_check)}")

    def is_missing(row):
        return all(
            pd.isna(val) or str(val).strip() == "" or val in [" ", ""]
            for val in row
        )

    initial_responses = len(df)
    mask = ~df[columns_to_check].apply(is_missing, axis=1)
    cleaned_df = df[mask]

    cleaned_responses = len(cleaned_df)
    removed_rows = initial_responses - cleaned_responses

    print(f"Number of initial responses: {initial_responses}")
    print(f"Number of cleaned responses: {cleaned_responses}")
    print(f"Number of rows removed: {removed_rows}")

    return cleaned_df, cleaned_responses

def delete_unnecessary_variables(df, meta, variables_to_delete):
    print(f"Attempting to delete the following variables: {variables_to_delete}")
    existing_variables = [var for var in variables_to_delete if var in df.columns]
    print(f"Variables found for deletion: {existing_variables}")

    df = df.drop(columns=existing_variables, errors="ignore")
    updated_columns = list(df.columns)
    keep_indices = [i for i, name in enumerate(meta.column_names) if name in updated_columns]

    meta.column_names = updated_columns
    meta.column_labels = [meta.column_labels[i] for i in keep_indices]
    meta.variable_value_labels = {k: v for k, v in meta.variable_value_labels.items() if k in updated_columns}
    meta.variable_display_width = {k: v for k, v in meta.variable_display_width.items() if k in updated_columns}
    meta.variable_measure = {k: v for k, v in meta.variable_measure.items() if k in updated_columns}
    meta.original_variable_types = {k: v for k, v in meta.original_variable_types.items() if k in updated_columns}
    meta.variable_storage_width = {k: v for k, v in meta.variable_storage_width.items() if k in updated_columns}

    print(f"Variables deleted successfully. Remaining variables: {updated_columns}")
    return df, meta

def process_and_clean_file_with_metadata(file_path, output_path, start_index, end_index, variables_to_delete=None):
    print(f"Reading file: {file_path}")
    data, meta = pyreadstat.read_sav(file_path)
    print(f"Original metadata variable count: {len(meta.column_names)}")

    # Ensure original_variable_types exists in metadata
    if not hasattr(meta, "original_variable_types"):
        meta.original_variable_types = {var: "unknown" for var in meta.column_names}

    cleaned_data, cleaned_responses = clean_sysmis_responses(data, start_index, end_index)

    if variables_to_delete:
        cleaned_data, meta = delete_unnecessary_variables(cleaned_data, meta, variables_to_delete)

    metadata_kwargs = {
        "variable_value_labels": meta.variable_value_labels,
        "column_labels": meta.column_labels,
        "variable_display_width": meta.variable_display_width,
        "variable_measure": meta.variable_measure,
        "variable_format": meta.original_variable_types,
    }

    print("Saving cleaned file with metadata...")
    pyreadstat.write_sav(
        cleaned_data,
        output_path,
        **metadata_kwargs
    )
    print(f"File saved to: {output_path}")
    return cleaned_data, meta, cleaned_responses

def add_variables_to_dataframe(df, meta, new_variables):
    print("Adding new variables to the DataFrame...")
    for var_name, var_info in new_variables.items():
        df[var_name] = var_info.get('data', [None] * len(df))
        if var_name not in meta.column_names:
            meta.column_names.append(var_name)
            meta.column_labels.append(var_info.get('label', var_name))
            if 'value_labels' in var_info:
                meta.variable_value_labels[var_name] = var_info['value_labels']
            if 'measure' in var_info:
                meta.variable_measure[var_name] = var_info['measure']
            if 'display_width' in var_info:
                meta.variable_display_width[var_name] = var_info['display_width']
            if 'original_variable_type' in var_info:
                meta.original_variable_types[var_name] = var_info['original_variable_type']
           
    print(f"New variables added: {list(new_variables.keys())}")
    return df, meta

def reorder_variables(df, meta, order):
    """
    Reorder variables in the DataFrame and update metadata to match the new order.
    
    Args:
        df (pd.DataFrame): The DataFrame to reorder.
        meta (pyreadstat.metadata_container): Metadata for the DataFrame.
        order (list): The desired variable order. Variables not in the list will be appended.
    
    Returns:
        df, meta: Updated DataFrame and metadata with reordered variables.
    """
    print("Reordering variables...")

    # Ensure all specified variables exist in the DataFrame
    available_order = [var for var in order if var in df.columns]
    remaining_vars = [var for var in df.columns if var not in available_order]

    # New column order
    new_order = available_order + remaining_vars

    # Reorder DataFrame columns
    df = df[new_order]

    # Update metadata attributes to match the new order
    keep_indices = [meta.column_names.index(var) for var in new_order]
    meta.column_names = new_order
    meta.column_labels = [meta.column_labels[i] for i in keep_indices]
    meta.variable_value_labels = {k: v for k, v in meta.variable_value_labels.items() if k in new_order}
    meta.variable_display_width = {k: meta.variable_display_width[k] for k in new_order if k in meta.variable_display_width}
    meta.variable_measure = {k: meta.variable_measure[k] for k in new_order if k in meta.variable_measure}
    meta.original_variable_types = {k: meta.original_variable_types[k] for k in new_order if k in meta.original_variable_types}
    meta.variable_storage_width = {k: meta.variable_storage_width[k] for k in new_order if k in meta.variable_storage_width}

    print(f"Variables reordered: {new_order}")
    return df, meta

def adjust_variable_storage(df, meta, start_index=29):
    """
    Adjust storage width and variable types based on SPSS requirements:
    - Numeric (F) variables: F8.0 format with storage width 8
    - String (A) variables: A16000 format with storage width 16000
    """
    print("Adjusting storage width and variable types...")
         # Initialize metadata dictionaries if they don't exist
    if not hasattr(meta, 'variable_storage_width'):
        meta.variable_storage_width = {}
    if not hasattr(meta, 'original_variable_types'):
        meta.original_variable_types = {}
    if not hasattr(meta, 'variable_display_width'):
        meta.variable_display_width = {}
    for idx, column_name in enumerate(meta.column_names[start_index:], start=start_index):
        if column_name in df.columns:
            if column_name in meta.original_variable_types:
                var_type = meta.original_variable_types[column_name]
                if var_type.startswith("F"):  # Numeric
                    meta.variable_storage_width[column_name] = 8
                    meta.original_variable_types[column_name] = "F8.0"
                elif var_type.startswith("A"):  # String
                    meta.variable_storage_width[column_name] = 16000
                    meta.original_variable_types[column_name] = "A16000"
            else:
                print(f"Variable {column_name} type not found in metadata, skipping.")

    # Verify changes
    print("\nVerifying metadata changes:")
    print("Sample string variable formats:")
    string_vars = [name for name in df.columns if not pd.api.types.is_numeric_dtype(df[name])][:5]
    for var in string_vars:
        print(f"{var}: format={meta.original_variable_types[var]}, storage_width={meta.variable_storage_width[var]}")

    return df, meta

def synchronize_metadata_with_dataframe(df, meta):
    print("Synchronizing metadata with the DataFrame...")
    updated_columns = list(df.columns)
    keep_indices = [i for i, name in enumerate(meta.column_names) if name in updated_columns]

    meta.column_names = updated_columns
    meta.column_labels = [meta.column_labels[i] for i in keep_indices]
    meta.variable_value_labels = {k: v for k, v in meta.variable_value_labels.items() if k in updated_columns}
    meta.variable_display_width = {k: v for k, v in meta.variable_display_width.items() if k in updated_columns}
    meta.variable_measure = {k: v for k, v in meta.variable_measure.items() if k in updated_columns}
    meta.original_variable_types = {k: v for k, v in meta.original_variable_types.items() if k in updated_columns}
    meta.variable_storage_width = {k: v for k, v in meta.variable_storage_width.items() if k in updated_columns}

    print(f"Metadata synchronized. Remaining columns: {updated_columns}")
    return meta

def main():
    start_index = 30
    end_index = 48

    global SURVEY_RESPONSE_MEMBER, SURVEY_RESPONSE_SPOUSE

# Process and Clean Member File
    member_file_path = os.path.join(BASE_PATH, MEMBER_FILE)
    member_output_path = os.path.join(BASE_PATH, "Cleaned_Member.sav")
    member_data, member_meta, member_responses = process_and_clean_file_with_metadata(
        member_file_path, member_output_path, start_index, end_index, UNNECESSARY_VARIABLES
    )
    SURVEY_RESPONSE_MEMBER = member_responses  # Update global variable
# Rename variable in Member File
    member_data, member_meta = rename_variable(member_data, member_meta, "Duration__in_seconds_", "ResponseDurationSeconds")

# Process and Clean Spouse File
    spouse_file_path = os.path.join(BASE_PATH, SPOUSE_FILE)
    spouse_output_path = os.path.join(BASE_PATH, "Cleaned_Spouse.sav")
    spouse_data, spouse_meta, spouse_responses = process_and_clean_file_with_metadata(
        spouse_file_path, spouse_output_path, start_index, end_index, UNNECESSARY_VARIABLES
    )
    SURVEY_RESPONSE_SPOUSE = spouse_responses  # Update global variable
# Rename variable in Spouse File
    spouse_data, spouse_meta = rename_variable(spouse_data, spouse_meta, "Duration__in_seconds_", "ResponseDurationSeconds")

# Update Global Variables
    TOTAL_RESPONSE_COUNT = SURVEY_RESPONSE_MEMBER + SURVEY_RESPONSE_SPOUSE

    print("Merging cleaned files...")
    merged_data = pd.concat([member_data, spouse_data], ignore_index=True)

    # Define Variables to Add
    new_variables = {
        "SurveyType": {
            "data": [SURVEY_TYPE] * len(merged_data),
            "label": "SurveyType",
            "display_width": 15,
            "measure": "nominal",
            "original_variable_type": "A200"
        },
        "RespondentType": {
            "data": [RESPONSE_TYPE] * len(merged_data),
            "label": "RespondentType",
            "display_width": 15,
            "measure": "nominal",
            "original_variable_type": "A200"
        },
        "ClubIndex": {
            "data": [CLUB_INDEX] * len(merged_data),
            "label": "ClubIndex",
            "display_width": 15,
            "measure": "nominal",
            "original_variable_type": "A200"
        },
        "ClubName": {
            "data": [CLUB_NAME] * len(merged_data),
            "label": "ClubName",
            "display_width": 15,
            "measure": "nominal",
            "original_variable_type": "A200"
        },
        "ClubAddressCity": {
            "data": [CLUB_ADDRESS_CITY] * len(merged_data),
            "label": "ClubAddressCity",
            "display_width": 15,
            "measure": "nominal",
            "original_variable_type": "A200"
        },
        "ClubAddressState": {
            "data": [CLUB_ADDRESS_STATE] * len(merged_data),
            "label": "ClubAddressState",
            "display_width": 15,
            "measure": "nominal",
            "original_variable_type": "A200"
        },
        "ClubAddressZip": {
            "data": [CLUB_ADDRESS_ZIP] * len(merged_data),
            "label": "ClubAddressZip",
            "display_width": 15,
            "measure": "nominal",
            "original_variable_type": "A200"
        },
        "ClubCategory": {
            "data": [CLUB_CATEGORY] * len(merged_data),
            "label": "ClubCategory",
            "display_width": 15,
            "measure": "nominal",
            "original_variable_type": "A32"
        },
        "DueDate": {
            "data": [DUE_DATE] * len(merged_data),
            "label": "DueDate",
            "display_width": 5,
            "measure": "scale",
            "original_variable_type": "DATETIME11"
        },
        "ClubType": {
            "data": [CLUB_TYPE] * len(merged_data),
            "label": "ClubType",
            "display_width": 5,
            "measure": "scale",
            "original_variable_type": "F8.0"
        },
        "ClubAddressRegion": {
            "data": [CLUB_ADDRESS_REGION] * len(merged_data),
            "label": "ClubAddressRegion",
            "display_width": 5,
            "measure": "scale",
            "original_variable_type": "F8.0"
        },
        "ClubTotalMemberCount": {
            "data": [TOTAL_MEMBER_COUNT] * len(merged_data),
            "label": "ClubTotalMemberCount",
            "display_width": 5,
            "measure": "scale",
            "original_variable_type": "F8.0"
        },
        "ClubTotalSpouseCount": {
            "data": [TOTAL_SPOUSE_COUNT] * len(merged_data),
            "label": "ClubTotalSpouseCount",
            "display_width": 5,
            "measure": "scale",
            "original_variable_type": "F8.0"
        },
        "ClubResponseMember": {
            "data": [SURVEY_RESPONSE_MEMBER] * len(merged_data),
            "label": "ClubResponseMember",
            "display_width": 5,
            "measure": "scale",
            "original_variable_type": "F8.0"
        },
        "ClubResponseSpouse": {
            "data": [SURVEY_RESPONSE_SPOUSE] * len(merged_data),
            "label": "ClubResponseSpouse",
            "display_width": 5,
            "measure": "scale",
            "original_variable_type": "F8.0"
        },
        "TotalClubResponse": {
            "data": [TOTAL_RESPONSE_COUNT] * len(merged_data),
            "label": "TotalClubResponse",
            "display_width": 5,
            "measure": "scale",
            "original_variable_type": "F8.0"
        },
        "ClubMainInitiationFee": {
            "data": [None] * len(merged_data),
            "label": "ClubMainInitiationFee",
            "display_width": 5,
            "measure": "scale",
            "original_variable_type": "DOLLAR12.0"
        },
        "ClubMainAnnualFee": {
            "data": [None] * len(merged_data),
            "label": "ClubMainAnnualFee",
            "display_width": 5,
            "measure": "scale",
            "original_variable_type": "DOLLAR12.0"
        }
    }

    merged_data, member_meta = add_variables_to_dataframe(merged_data, member_meta, new_variables)
    merged_meta = synchronize_metadata_with_dataframe(merged_data, member_meta)

# Define the desired variable order
    desired_order = [
        "ResponseId", "IPAddress", "LocationLatitude", "LocationLongitude", 
    "ResponseDurationSeconds", "Progress", "Finished", "StartDate", "EndDate", 
    "RecordedDate", "DueDate", "SurveyType", "RespondentType", "ClubIndex", 
    "ClubName", "ClubCategory", "ClubTotalMemberCount", "ClubTotalSpouseCount", 
    "ClubResponseMember", "ClubResponseSpouse", "TotalClubResponse", 
    "ClubMainInitiationFee", "ClubMainAnnualFee", "ClubType", "ClubAddressRegion", 
    "ClubAddressCity", "ClubAddressState", "ClubAddressZip"
    ]

# Reorder variables in the merged DataFrame and metadata
    merged_data, merged_meta = reorder_variables(merged_data, merged_meta, desired_order)

# Adjust variable storage for variables starting at position 29
    merged_data, merged_meta = adjust_variable_storage(merged_data, merged_meta, start_index=29)

# Ensure metadata and DataFrame are synchronized after changes
    merged_meta = synchronize_metadata_with_dataframe(merged_data, merged_meta)

    if len(merged_meta.column_labels) != len(merged_data.columns):
        merged_meta.column_labels = merged_data.columns.tolist()

    print("Metadata before saving:")
    print("Variable Display Widths:", merged_meta.variable_display_width)
    print("Variable Formats:", merged_meta.original_variable_types)
    print("Variable Storage Widths:", merged_meta.variable_storage_width)

    output_file_path = os.path.join(BASE_PATH, OUTPUT_FILE)
    print("Saving merged file with metadata...")
    pyreadstat.write_sav(
        merged_data,
        output_file_path,
        variable_value_labels=merged_meta.variable_value_labels,
        column_labels=merged_meta.column_labels,
        variable_display_width=merged_meta.variable_display_width,
        variable_measure=merged_meta.variable_measure,
        variable_format=merged_meta.original_variable_types
    )
    print(f"Merged file saved to: {output_file_path}")

if __name__ == "__main__":
    main()
