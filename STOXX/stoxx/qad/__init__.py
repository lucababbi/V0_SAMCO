import pyodbc
creds = 'DRIVER={SQL Server};SERVER=delacroix.prod.ci.dom;DATABASE=qai;UID=stx-ro;PWD=stx-ro'
#creds = 'DRIVER={SQL Server};SERVER=mpzhwindex01;DATABASE=qai;UID=stx-ro;PWD=stx-ro'
con = pyodbc.connect(creds)
