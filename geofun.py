from OSMPythonTools.overpass import Overpass
from OSMPythonTools.nominatim import Nominatim
from OSMPythonTools.overpass import overpassQueryBuilder
import pandas as pd

overpass = Overpass()
nominatim = Nominatim()

my_brand = "Burberry"
query = overpassQueryBuilder(area=nominatim.query('England'), elementType=['way', 'node'], selector=[f'"name"~"{my_brand}"'
    , '"shop"="clothes"'
                                                                                                     ])

supermarkets = overpass.query(query, timeout=600)


my_list = []
for supermarket in supermarkets.elements():
    print(f"--- {supermarket.tags()} ---")
    if supermarket.type()=="way":
        first_node = supermarket.nodes()[0]
        lat, lon = first_node.lat(), first_node.lon()
    my_dict = dict(
        lat=supermarket.lat() or lat,
        lon=supermarket.lon() or lon,
        **supermarket.tags()
    )
    my_list.append(my_dict)


df = pd.DataFrame(my_list)
num_rows = df.shape[0]

interesting_cols = []
for my_col in df.columns:
    grouped = df.groupby(my_col)
    size = grouped.size()
    # If there at least 5 of a kind...
    if size.iloc[0] > 5:
        print(f"---{size}---")
        interesting_cols.append(my_col)
    # If the attribute is filled in at least 25% of the time...
    if size.sum() > num_rows*.25:
        print(f"{my_col} : {size.sum()}")
        if my_col not in interesting_cols:
            interesting_cols.append(my_col)

final_df = df.loc[:, interesting_cols]
final_df.to_csv(f"{my_brand} in England.csv", index=False)
print(f"All done!")
