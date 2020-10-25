import sys
import shutil
source_path_notebooks = "D:/code/da-notebooks/notebooks/_modules"
dest_path_notebooks = "D:/code/travis2/notebooks_to_test"
shutil.copytree(source_path_notebooks, dest_path_notebooks)

