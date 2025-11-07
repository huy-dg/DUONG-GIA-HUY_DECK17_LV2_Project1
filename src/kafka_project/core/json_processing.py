# import ijson  # Lib to parse large JSON file_path
# import time  # Time lib to measure execution time
import os
import json

# import jsonlines

# -----------------------------------------------------------------------------#
# -----------------------------------------------------------------------------#


def save_to_json(data, output_dir=None, save_name=None):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    file_path = os.path.join(output_dir, f"{save_name}.json")
    try:
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)
        print(f"Saved to {file_path}")
    except Exception as e:
        print(f"Error saving to JSON: {e}")


# -----------------------------------------------------------------------------#
# -----------------------------------------------------------------------------#

if __name__ == "__main__":
    pass


# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||#
# -----------------------------------------------------------------------------#

"""
# Read from json file_path pool and streaming to batching_data function
def streaming_json(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as file_path:
            data = ijson.items(file_path, "item")
            yield from data
    except Exception as e:
        print(f"Error reading file_path {file_path}: {e}")


# receiving file_path from streaming_json and put data into batches
def batching_data(file_path, batch_size):
    data = []
    batch_no = 0
    try:
        with open(file_path, "r", encoding="utf-8") as file_path:
            documents = ijson.items(file_path, "item")
            for doc in documents:
                data.append(doc)
                if len(data) == batch_size:
                    batch_no += 1
                    yield data.copy(), batch_no
                    data.clear()
            if data:
                batch_no += 1
                yield data.copy(), batch_no
                data.clear()
    except Exception as e:
        print(f"Error openning file_path: {e}")


def product_id_paginate(doc, product_dict):
    id = doc["id"]
    url = doc["url"]
    if id in product_dict and len(product_dict[id]) < 100:
        product_dict[id].append(url)
    elif id in product_dict and len(product_dict[id]) > 100:
        pass
    else:
        product_dict[id] = [url]
    return product_dict

"""

# -----------------------------------------------------------------------------#

## def main():
"""
    file_pool = [
        "view_product_detail_L.json",
        "select_product_option_L.json",
        "select_product_option_quality_L.json",
        "add_to_cart_action.json",
        "product_detail_recommendation_visible.json",
        "product_detail_recommendation_noticed.json",
        "product_view_all_recommend_clicked.json",
    ]

    name_pool = ["data1", "data2", "data3", "data4"]

    file_dir = "D:\\glamira-data\\scraped data\\"

    ## Convert json to jsonlines -------------------------------------------
    for file_name in name_pool:
        if not os.path.exists(file_dir):
            os.makedirs(file_dir)
        source_path = os.path.join(file_dir, f"{file_name}.json")
        with open(source_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        destination_path = os.path.join(file_dir, f"{file_name}.jsonl")
        with jsonlines.open(destination_path, mode="w") as writer:
            for item in data:
                if item and len(item) > 1:
                    if "value" in item.keys():
                        item["name"] = item.pop("value")
                    writer.write(item)

        print(f"Converted {source_path} to {destination_path}.")
    ##----------------------------------------------------------------------
    """
"""
    test_data = [
        {"id": "1234", "url": "url1"},
        {"id": "1235", "url": "url2"},
        {"id": "1236", "url": "url3"},
        {"id": "1234", "url": "url4"},
        {"id": "1237", "url": "url5"},
        {"id": "1236", "url": "url6"},
    ]
    """
## Transform data from MongoDB, group all URL to product_id -----------
# product_id = {}
# output_dir = "product_dict\\"
# final = []

# start = time.perf_counter()
# for doc in streaming_json(name_pool):
#     result = product_id_paginate(doc, product_dict=product_id)

# for id in result.keys():
#     result[id].sort()

# for id, url_list in result.items():
#     final.append({f"{id}": url_list})

# save_to_json(final, output_dir)

# end = time.perf_counter()
# print(f"Total processing time: {end - start} seconds.")
##----------------------------------------------------------------------

## batching data from final.json to scrape in multiple VMs--------------
# for data, batch_no in batching_data("product_dict\\batch_4_div_2.json", 100):
#     save_to_json(data, output_dir="product_dict", save_name=f"4_div_2.{batch_no}")
##----------------------------------------------------------------------

## Count the successfully scraped data----------------------------------
# count_data = 0
# failed_data = 0
# for doc in streaming_json(file_pool):
#     count_data += 1
#     if doc is None:
#         failed_data += 1
# print(count_data)
# print(failed_data)
##----------------------------------------------------------------------
