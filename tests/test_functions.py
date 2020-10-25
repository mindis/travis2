
### import home.matias.code.travistest.setup.py
###import tests.test_setup

def test_setup():

    ### after importing this module, and if completed successfully,  
    ### finished_successfully should be True
    print('importing setup...')
    import setup
    assert setup.finished_successfully == True


