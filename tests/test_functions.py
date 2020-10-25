
### import home.matias.code.travistest.setup.py
###import tests.test_setup

def test_my_unit_is_2():
    print('Running test_my_unit_is_2...')
    print('importing my_unit..')
    import my_unit
    print('import finished. Will assert...')

    assert main.x == 2


def test_my_unit_is_int():
    print('Running test_my_unit_is_int...')
    print('importing my_unit..')
    import my_unit
    print('import finished. Will assert...')

    assert isinstance(main.x, int) 
    
    


