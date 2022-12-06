find . -size +0G -name '*zoom_event_*202?-*.csv' -exec cat  {} + > zoom_event_data.csv
find . -size +0G -name '*zoom_part_*202?-*.csv' -exec cat  {} + > zoom_part_data.csv

