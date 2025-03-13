import dlt
import requests
from typing import Dict, List, Any, Iterator
           
def fetch_github_json(url: str) -> Dict[str, Any]:
    """
    Fetches JSON data from GitHub raw content URL.
       
    Args:
        url: GitHub URL to the JSON file
    
    Returns:
        Parsed JSON data as a dictionary
    """
    # Convert GitHub UI URL to raw content URL
    if "github.com" in url and "/blob/" in url:
        raw_url = url.replace("github.com", "raw.githubusercontent.com").replace("/blob/", "/")
    else:
        raw_url = url
        
    response = requests.get(raw_url)
    response.raise_for_status()  # Raise exception for failed requests
       
    return response.json()

@dlt.source
def github_json_source(url: str = "https://github.com/jokecamp/FootballData/blob/master/EPL%201992%20-%202015/2015/2015-08-15.all-epl-games.json"):
    """
    DLT source that loads JSON data from GitHub.
    
    Args:
        url: GitHub URL to the JSON file
    
    Yields:
        Resource containing the JSON data
    """
    @dlt.resource(name="epl_matches", write_disposition="replace")
    def get_epl_matches() -> Iterator[Dict[str, Any]]:
        """
        Resource for loading EPL match data from GitHub.
        
        Returns:
            Iterator of match data dictionaries
        """
        data = fetch_github_json(url)
        
        # Check if data is a list of items or a dictionary
        if isinstance(data, list):
            for item in data:
                yield item
        else:
            # If it's a dictionary with a key containing the list
            # Inspect the data structure and yield accordingly
            for key, value in data.items():
                if isinstance(value, list):
                    for item in value:
                        yield item
                else:
                    yield {key: value}

# Create pipeline
pipeline = dlt.pipeline(
    pipeline_name="github_epl_data",
    destination="duckdb",
    dataset_name="football_data"
)
    
# Run the pipeline
load_info = pipeline.run(github_json_source())

# Print load info
print(load_info)

# Example query to verify loaded data
with pipeline.default_schema.engine.connect() as conn:
    query = "SELECT * FROM football_data.epl_matches LIMIT 5"
    result = conn.execute(query).fetchall()
    for row in result:
        print(row)
       
