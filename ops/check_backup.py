from tarfile import TarFile
import pandas as pd
import os
from pathlib import Path

# Look for backup files in this dir
base_dir = Path("/data2/backup/") 
file_list = []

# Walk through all backup files
for backupfile in  os.listdir(base_dir):
    with (TarFile.open(base_dir / backupfile)) as t:
        my_files = t.getmembers()
    print(f"{backupfile} : {len(my_files)} files found.")

    # Put files in a DataFrame
    for f in my_files:
        if f.size:
            file_list.append(dict(
                aname=backupfile,
                fname=f.name,
                ftop=f.name.split('/')[1], # top level dir
                fsec=f.name.split('/')[2], # second level dir
                fsize=f.size))

df = pd.DataFrame(file_list)

# Use pandas group by to calculate number of files 
no_of_files = df.groupby(('aname', 'ftop', 'fsec')).size()

# Sum up the file size by directory (level 2)
file_size = df.groupby(('aname', 'ftop', 'fsec')).sum().fsize

# Print only the (uncompressed) file size of directories with more than 2 files
# Divide by 1 MB
print("Directory sizes in MB (uncompressed)")
print(file_size[no_of_files>2] / (1<<20))

