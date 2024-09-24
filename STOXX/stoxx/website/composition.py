import requests
import os
import keyring 

url = 'http://www.stoxx.com/download/data/composition_files/{index}/{ftype}_{index}_{day}.csv'
login_url = 'http://www.stoxx.com//mystoxx/user_profile.html'

username = 'nicola.palumbo@stoxx.com'
password = keyring.get_password('stoxx_website', username)
credentials = username, password

save_in_dir = os.path.dirname(os.path.realpath(__file__))

  
def get_composition_website(indexsymbol, date, type_):
    """Downloads the files from Stoxx web site
    from from_date to to_date
    """
    user_pass = dict(username=username, password=password)
    rr = requests.post(login_url,stream=True, data=user_pass)
    
    params = dict(indexsymbol.lower(), day=date, ftype=type_)

    rr = requests.get(url.format(**params),stream=True, auth=credentials) 
    if rr.headers['content-type'] != 'text/csv':
        pass
    print(url.format(**params), rr.status_code, rr.headers['content-type'])    