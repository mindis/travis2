
### import home.matias.code.travistest.setup.py
###import tests.test_setup


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
    
    


