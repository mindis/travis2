
### import home.matias.code.travistest.setup.py
###import tests.test_setup

import sys
import shutil
# insert at 1, 0 is the script path (or '' in REPL)
source_path_notebooks = "D:/code/da-notebooks/notebooks/_modules"
dest_path_notebooks = "D:/code/travis2/notebooks_to_test"
shutil.copytree(source_path_notebooks, dest_path_notebooks)

path_notebooks = 'notebooks_to_test'


def test_notebooks_compatible_with_python_2():
    import os 
    finished = False
    for notebook in os.listdir(path_notebooks):
        import notebook
        print(f'imported {notebook}')
    
    finished = True
    assert finished


def test_my_unit_is_2():
    print('Running test_my_unit_is_2...')
    print('importing my_unit..')
    import my_unit
    print('import finished. Will assert...')

    assert my_unit.x == 2


def test_my_unit_is_int():
    print('Running test_my_unit_is_int...')
    print('importing my_unit..')
    import my_unit
    print('import finished. Will assert...')

    assert isinstance(my_unit.x, int) 
    
    


