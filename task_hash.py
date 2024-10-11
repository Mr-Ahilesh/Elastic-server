import requests
import pandas as pd
import pysolr
import json

# Directory paths for Solr core and CSV data
core_dir = r"C:\Users\Ahile\Downloads\solr-8.11.4\solr-8.11.4\server\solr\name"
csv_file = r"C:\Users\Ahile\OneDrive\Desktop\task\Employee Sample Data 1.csv"

# Create a collection (Solr core)
def createCollection(p_collection_name):
    solr_url = "http://localhost:8983/solr/admin/cores?action=CREATE"
    params = {
        'name': p_collection_name,
        'instanceDir': core_dir,
        'configSet': '_default',
    }
    try:
        response = requests.get(solr_url, params=params)
        if response.status_code == 200:
            print(f"Core '{p_collection_name}' created successfully.")
        else:
            print(f"Failed to create core. Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print(f"Error occurred: {e}")

# Index data to Solr, excluding a specified column
def indexData(p_collection_name, p_exclude_column):
    # Connect to the Solr instance
    solr = pysolr.Solr(f'http://localhost:8983/solr/{p_collection_name}', always_commit=True)
    
    # Load employee data from CSV
    df = pd.read_csv(csv_file)
    
    # Remove the excluded column
    df = df.drop(columns=[p_exclude_column])

    # Convert data to a JSON format suitable for Solr indexing
    data_to_index = json.loads(df.to_json(orient="records"))
    
    # Index data to Solr
    solr.add(data_to_index)
    print(f"Data indexed into collection {p_collection_name}, excluding column '{p_exclude_column}'")

# Search by a specific column and value
def searchByColumn(p_collection_name, p_column_name, p_column_value):
    solr = pysolr.Solr(f'http://localhost:8983/solr/{p_collection_name}', always_commit=True)
    query = f'{p_column_name}:{p_column_value}'
    results = solr.search(query)
    for result in results:
        print(result)

# Get the total number of employees (documents) in the collection
def getEmpCount(p_collection_name):
    solr = pysolr.Solr(f'http://localhost:8983/solr/{p_collection_name}', always_commit=True)
    results = solr.search('*:*')
    return results.hits

# Delete an employee by their Employee ID
def delEmpById(p_collection_name, p_employee_id):
    solr = pysolr.Solr(f'http://localhost:8983/solr/{p_collection_name}', always_commit=True)
    solr.delete(id=p_employee_id)
    print(f"Employee with ID {p_employee_id} deleted from collection {p_collection_name}")

# Get the department facet (group employees by department)
def getDepFacet(p_collection_name):
    solr = pysolr.Solr(f'http://localhost:8983/solr/{p_collection_name}', always_commit=True)
    params = {
        'facet': 'on',
        'facet.field': 'Department'
    }
    results = solr.search('*:*', **params)
    print(f"Department Facet: {results.facets['facet_fields']['Department']}")


# Example execution

v_nameCollection = 'Hash_Ahilesh'
v_phoneCollection = 'Hash_1234'

# Step 1: Create collections
createCollection(v_nameCollection)
createCollection(v_phoneCollection)

# Step 2: Get employee count
print("Initial employee count:", getEmpCount(v_nameCollection))

# Step 3: Index data, excluding the 'Department' and 'Gender' columns respectively
indexData(v_nameCollection, 'Department')
indexData(v_phoneCollection, 'Gender')

# Step 4: Delete employee by ID
delEmpById(v_nameCollection, 'E02003')

# Step 5: Get employee count after deletion
print("Employee count after deletion:", getEmpCount(v_nameCollection))

# Step 6: Search records by column
print("Search results for Department = IT:")
searchByColumn(v_nameCollection, 'Department', 'IT')

print("Search results for Gender = Male:")
searchByColumn(v_nameCollection, 'Gender', 'Male')

print("Search results in phone collection for Department = IT:")
searchByColumn(v_phoneCollection, 'Department', 'IT')

# Step 7: Get department facet
print("Department facet in name collection:")
getDepFacet(v_nameCollection)

print("Department facet in phone collection:")
getDepFacet(v_phoneCollection)
