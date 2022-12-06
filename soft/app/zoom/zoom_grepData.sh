grep -r  --include '*.csv' . |  sort -k 5 -t ',' | grep lifering_zoom_part_data_202 | sed 's/lifering_zoom_part_data_/data_/g' | sed 's/\/old_2021//g' 
